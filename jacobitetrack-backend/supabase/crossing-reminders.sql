alter table public.jacobite_push_subscriptions add column if not exists crossing_at timestamptz, add column if not exists device_id text;
update public.jacobite_push_subscriptions set device_id=id where device_id is null;
create index if not exists jacobite_push_device_idx on public.jacobite_push_subscriptions(device_id);
create index if not exists jacobite_push_crossing_idx on public.jacobite_push_subscriptions(crossing_at) where crossing_at is not null;
