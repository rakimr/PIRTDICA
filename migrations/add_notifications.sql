BEGIN;

ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.email_queue ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "notifications_anon_select" ON public.notifications;
DROP POLICY IF EXISTS "email_queue_block_anon" ON public.email_queue;

CREATE POLICY "notifications_anon_select" ON public.notifications
    FOR SELECT TO anon
    USING (false);

CREATE POLICY "email_queue_block_anon" ON public.email_queue
    FOR SELECT TO anon
    USING (false);

COMMIT;
