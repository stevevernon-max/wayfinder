import webpush from "npm:web-push@3.6.7";
import { createHash, timingSafeEqual } from "node:crypto";
import { Buffer } from "node:buffer";

const origin = "https://stevevernon-max.github.io";
const feedUrl = origin + "/wayfinder/jacobitetrack/feed.json";
const url = Deno.env.get("SUPABASE_URL")!;
const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const subscriptions = "jacobite_push_subscriptions";
const deliveries = "jacobite_push_deliveries";
const viewpoints: Record<string, string> = {
  glenfinnan: "Glenfinnan Viaduct", lochailort: "Lochailort",
  arisaig: "Arisaig", morar: "Morar",
};
const hash = (s: string) => createHash("sha256").update(s).digest("hex");
const reply = (data: unknown, status = 200) => Response.json(data, { status, headers: {
  "Access-Control-Allow-Origin": origin, "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type", "Cache-Control": "no-store",
}});

async function db(path: string, method = "GET", data?: unknown, prefer?: string) {
  const response = await fetch(url + "/rest/v1/" + path, {
    method, headers: { apikey: key, Authorization: "Bearer " + key, "Content-Type": "application/json", ...(prefer ? { Prefer: prefer } : {}) },
    body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(7000),
  });
  if (!response.ok) throw Error("Database request failed: " + response.status);
  const body = await response.text();
  return body ? JSON.parse(body) : null;
}

function validSubscription(sub: any) {
  try {
    const endpoint = new URL(sub.endpoint);
    return sub.endpoint.length < 2048 && endpoint.protocol === "https:" && !endpoint.port &&
      !endpoint.username && !endpoint.password && !endpoint.hash &&
      (endpoint.hostname === "web.push.apple.com" || endpoint.hostname === "fcm.googleapis.com" ||
        endpoint.hostname === "updates.push.services.mozilla.com") &&
      /^[\w-]{87}$/.test(sub.keys?.p256dh) && /^[\w-]{22}$/.test(sub.keys?.auth);
  } catch { return false; }
}

async function send(sub: any, title: string, body: string, tag: string, settings: any) {
  await webpush.sendNotification(sub, JSON.stringify({ title, body, tag }), {
    vapidDetails: { subject: origin + "/wayfinder/jacobitetrack/", publicKey: settings.jacobite_vapid_public, privateKey: settings.jacobite_vapid_private },
    TTL: 120, timeout: 6000,
  });
}

async function sendCrossingReminders(now: number, settings: any) {
  const imminent = new Date(now + 20 * 60000).toISOString();
  await db(subscriptions + "?crossing_at=lt." + new Date(now).toISOString(), "DELETE");
  let sent = 0;
  for (const record of await db(subscriptions + "?crossing_at=lte." + imminent + "&order=crossing_at.asc&limit=100")) {
    const remaining = (Date.parse(record.crossing_at) - now) / 60000;
    if (remaining <= 0 || remaining > record.lead) continue;
    const claim = await db(deliveries, "POST", { subscriber: record.id, event: "crossing-reminder" },
      "resolution=ignore-duplicates,return=representation");
    if (!claim.length) continue;
    try {
      await send(record.subscription, "Glenfinnan Viaduct crossing reminder",
        "Scheduled in " + Math.ceil(remaining) + " min.", record.id, settings);
      sent++;
    } catch (error: any) {
      if ([404, 410].includes(error.statusCode)) {
        await db(subscriptions + "?device_id=eq." + record.device_id, "DELETE");
      } else {
        await db(deliveries + "?subscriber=eq." + record.id + "&event=eq.crossing-reminder", "DELETE");
      }
    }
  }
  return sent;
}

async function monitor(settings: any) {
  const now = Date.now();
  const reminders = await sendCrossingReminders(now, settings);
  const response = await fetch(feedUrl + "?alert=" + now, { signal: AbortSignal.timeout(7000) });
  if (!response.ok) throw Error("Rail feed unavailable");
  const feed = await response.json();
  const age = now - Date.parse(feed.updated);
  if (!Number.isFinite(age) || age >= 600000 || age < -60000 || !Array.isArray(feed.trains)) {
    return reply({ checked: true, reason: "No fresh railway feed", reminders });
  }
  const trains = feed.trains.filter((t: any) => {
    const elapsed = now - Date.parse(t.observedAt);
    return typeof t.id === "string" && !t.cancelled && elapsed >= -60000 && elapsed < 600000;
  });
  await db(deliveries + "?created_at=lt." + new Date(now - 3 * 86400000).toISOString(), "DELETE");
  let sent = 0;
  for (const record of await db(subscriptions + "?crossing_at=is.null&select=*&limit=100")) {
    const due = trains.flatMap((train: any) => {
      const stationEstimate = train.viewpoints?.[record.viewpoint];
      let minutes = Number.isFinite(stationEstimate?.etaMinutes) ? stationEstimate.etaMinutes - age / 60000 : NaN;
      if (record.viewpoint === "glenfinnan") {
        const departure = train.glenfinnanDeparture || {};
        const at = Date.parse(departure.actualAt || departure.forecastAt || departure.scheduledAt);
        if (train.direction === "out" && Number.isFinite(at)) minutes = (at + 60000 - now) / 60000;
        else if (stationEstimate?.quality !== "estimated_between_station_times") return [];
      }
      return minutes > 0 && minutes <= record.lead ? [{ train, minutes }] : [];
    }).sort((a: any, b: any) => a.minutes - b.minutes);
    for (const { train, minutes } of due) {
      const event = [train.id, record.viewpoint, new Date(now + minutes * 60000).toISOString().slice(0, 10)].join("|");
      const claimed = await db(deliveries, "POST", { subscriber: record.id, event }, "resolution=ignore-duplicates,return=representation");
      if (!claimed.length) continue;
      try {
        await send(record.subscription, "Train approaching " + viewpoints[record.viewpoint],
          "Estimated in " + Math.ceil(minutes) + " min.", event, settings);
        sent++;
      } catch (error: any) {
        if ([404, 410].includes(error.statusCode)) {
          await db(subscriptions + "?id=eq." + record.id, "DELETE");
          break;
        }
        await db(deliveries + "?subscriber=eq." + record.id + "&event=eq." + encodeURIComponent(event), "DELETE");
      }
    }
  }
  return reply({ checked: true, sent, reminders });
}

Deno.serve(async (request) => {
  const action = new URL(request.url).pathname.split("/").at(-1);
  const browserOrigin = request.headers.get("origin");
  try {
    if (request.method === "OPTIONS") return reply({});
    if (action === "monitor" && request.method === "POST") {
      const token = request.headers.get("x-jacobite-cron");
      if (!token || !/^[a-f0-9]{64}$/.test(token)) return reply({ error: "Forbidden" }, 403);
      const settings = await db("rpc/jacobite_push_settings", "POST", {});
      if (!timingSafeEqual(Buffer.from(token), Buffer.from(settings.jacobite_cron_token))) return reply({ error: "Forbidden" }, 403);
      return await monitor(settings);
    }
    if (action === "config" && request.method === "GET") {
      const settings = await db("rpc/jacobite_push_settings", "POST", {});
      return reply({ ready: true, publicKey: settings.jacobite_vapid_public });
    }
    if (request.method !== "POST" || browserOrigin !== origin) return reply({ error: "Forbidden" }, 403);
    if (!["subscribe", "unsubscribe"].includes(action!)) return reply({ error: "Not found" }, 404);
    const body = await request.text();
    if (body.length > 4096) return reply({ error: "Too large" }, 413);
    let data;
    try { data = JSON.parse(body); } catch { return reply({ error: "Invalid JSON" }, 400); }
    if (!data || typeof data !== "object") return reply({ error: "Invalid request" }, 400);
    if (action === "unsubscribe") {
      if (typeof data.endpoint !== "string" || data.endpoint.length > 2048) return reply({ error: "Invalid endpoint" }, 400);
      await db(subscriptions + "?device_id=eq." + hash(data.endpoint), "DELETE");
      return reply({ unsubscribed: true });
    }
    if (!validSubscription(data.subscription) || !Object.hasOwn(viewpoints, data.viewpoint) ||
        ![5, 10, 15, 20].includes(data.leadMinutes)) return reply({ error: "Invalid alert preferences" }, 400);
    const deviceId = hash(data.subscription.endpoint);
    const once = Object.hasOwn(data, "crossingAt");
    const now = Date.now();
    if (once && (data.viewpoint !== "glenfinnan" || !Number.isSafeInteger(data.crossingAt) ||
        data.crossingAt <= now || data.crossingAt > now + 15 * 86400000)) {
      return reply({ error: "Invalid crossing time" }, 400);
    }
    const crossingAt = once ? new Date(data.crossingAt).toISOString() : null;
    const id = once ? hash(deviceId + "|" + crossingAt) : deviceId;
    const existing = await db(subscriptions + "?select=id&device_id=eq." + deviceId + "&limit=1");
    if (!existing.length) {
      const settings = await db("rpc/jacobite_push_settings", "POST", {});
      try {
        await send(data.subscription, "JacobiteTrack alerts enabled", "This is a test banner for " + viewpoints[data.viewpoint] + ".", "jacobite-test", settings);
      } catch {
        return reply({ error: "The test notification could not be sent" }, 422);
      }
    }
    await db(subscriptions, "POST", { id, subscription: data.subscription, viewpoint: data.viewpoint, lead: data.leadMinutes, crossing_at: crossingAt, device_id: deviceId },
      "resolution=merge-duplicates,return=minimal");
    return reply({ subscribed: true }, 201);
  } catch {
    return reply({ error: "Alert service temporarily unavailable" }, 503);
  }
});
