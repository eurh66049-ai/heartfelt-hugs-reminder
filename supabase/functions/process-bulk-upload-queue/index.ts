// معالج طابور الرفع الخلفي
// يستدعى كل دقيقة من cron — يأخذ دفعة من جدول bulk_upload_queue
// ويستدعي bulk-upload-books-ai لمعالجتها، ثم يحدّث حالة كل صف.

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// الحد الأقصى لعدد الكتب المسموح بمعالجتها بالتوازي **لكل دفعة (batch_label)**.
// دالة claim_bulk_upload_items في قاعدة البيانات تطبّق هذا الحد لكل دفعة على حدة،
// لذلك إذا وُجدت 3 دفعات نشطة فقد يتم سحب حتى 30 كتاب في تشغيل واحد (10 لكل دفعة).
const PER_BATCH_LIMIT = 10;

interface QueueItem {
  id: string;
  title: string;
  book_file_url: string;
  cover_image_url: string | null;
  attempts: number;
  max_attempts: number;
  created_by_email: string | null;
}

interface BookResult {
  success?: boolean;
  duplicate?: boolean;
  retryable?: boolean;
  title?: string;
  error?: string;
  id?: string;
  page_count?: number | null;
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase = createClient(supabaseUrl, serviceKey);

  try {
    // 1) اختر دفعة وعلّمها كـ "processing" بشكل ذرّي
    const { data: claimed, error: claimError } = await supabase
      .rpc("claim_bulk_upload_items", { p_limit: PER_BATCH_LIMIT });

    if (claimError) {
      console.error("[Queue] claim error:", claimError);
      return new Response(JSON.stringify({ success: false, error: claimError.message }), {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const items = (claimed || []) as QueueItem[];
    if (items.length === 0) {
      return new Response(JSON.stringify({ success: true, processed: 0, message: "لا توجد عناصر معلّقة" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    console.log(`[Queue] ⚙️ معالجة ${items.length} كتاب من الطابور`);

    // 2) استدعِ دالة الرفع الذكية
    const aiResponse = await fetch(`${supabaseUrl}/functions/v1/bulk-upload-books-ai`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${serviceKey}`,
      },
      body: JSON.stringify({
        books: items.map((item) => ({
          title: item.title,
          book_file_url: item.book_file_url,
          cover_image_url: item.cover_image_url ?? undefined,
          user_email: item.created_by_email ?? "queue@kotobi.local",
        })),
      }),
    });

    let aiPayload: any = {};
    try {
      aiPayload = await aiResponse.json();
    } catch (_) {
      aiPayload = {};
    }

    const results: BookResult[] = Array.isArray(aiPayload?.results) ? aiPayload.results : [];

    // 3) حدّث حالة كل صف
    const nowIso = new Date().toISOString();
    let success = 0, failed = 0, requeued = 0, duplicates = 0;

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const result = results[i] || results.find((r) => r.title === item.title) || {
        success: false,
        retryable: !aiResponse.ok,
        error: aiPayload?.error || `HTTP ${aiResponse.status}`,
      };

      let nextStatus: string;
      if (result.success) {
        nextStatus = "success";
        success++;
      } else if (result.duplicate) {
        nextStatus = "duplicate";
        duplicates++;
      } else if (result.retryable && item.attempts < item.max_attempts) {
        nextStatus = "pending"; // أعِد للطابور
        requeued++;
      } else {
        nextStatus = "failed";
        failed++;
      }

      await supabase
        .from("bulk_upload_queue")
        .update({
          status: nextStatus,
          error: result.error ?? null,
          result_book_id: result.id ?? null,
          page_count: result.page_count ?? null,
          finished_at: nextStatus === "pending" ? null : nowIso,
          started_at: nextStatus === "pending" ? null : undefined,
        })
        .eq("id", item.id);
    }

    console.log(`[Queue] ✅ نجح ${success} • مكرر ${duplicates} • فشل ${failed} • أعيد ${requeued}`);

    return new Response(
      JSON.stringify({
        success: true,
        processed: items.length,
        summary: { success, failed, duplicates, requeued },
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (err) {
    console.error("[Queue] خطأ غير متوقع:", err);
    return new Response(
      JSON.stringify({ success: false, error: err instanceof Error ? err.message : "خطأ غير معروف" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
