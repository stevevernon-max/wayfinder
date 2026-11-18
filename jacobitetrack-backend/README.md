# JacobiteTrack background push service

The iPhone PWA is published at `/wayfinder/jacobitetrack/` using GitHub Pages. Its service worker, manifest, notification subscribe/update/disable UI and settings are committed.

The backend source is in this directory and is intended for a **separate** Netlify Functions project, on the existing Free team.

## Current deployment status (18 November 2026)

- Netlify project **jacobitetrack-alerts** was created.
- Its Git repository has NOT been linked or deployed.
- Setting environment variables using the connected Netlify tool returned HTTP 403. The credentials have not been configured.
- The railway feed for real train ETAs does not exist yet; Darwin/RDM provider access is still needed.
- Consequently no background train alerts can currently be delivered.

## Complete deployment

1. Connect Netlify project `jacobitetrack-alerts` to `stevevernon-max/wayfinder` and set **Base directory** to `jacobitetrack-backend`. Publish directory is `public`, functions directory `netlify/functions`.
2. Use the Netlify dashboard to generate/set **production Functions** secrets: `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, and `VAPID_CONTACT`. Generate a matching keypair using `npx web-push generate-vapid-keys`. The VAPID private key MUST NOT enter Git or frontend JavaScript.
3. Supply an authorised, verified train-data HTTPS feed URL as `RAIL_FEED_URL` and `RAIL_FEED_TOKEN` when needed.
4. Validate that the push backend can be reached publicly without Netlify site-load SSO. The public API paths are `/api/push/config`, `/api/push/subscribe`, `/api/push/unsubscribe`. The backend's response headers restrict browser access to the JacobiteTrack GitHub Pages origin.
5. After backend deployment, update `jacobitetrack/push-config.json` with `{"backendUrl":"https://jacobitetrack-alerts.netlify.app"}` (or the actual deployed HTTPS URL).
6. Open JacobiteTrack on iPhone via the **Add to Home Screen** icon. Use the app's Enable alerts button to subscribe, choose a viewpoint and lead time. Push needs user permission.
7. Verify against a **real** recent train report and check scheduled `push-monitor` execution and push delivery. Only claim success after an actual push arrives on a locked phone.

## Feed requirements

The railway feed must be licensed/authorised. Expected JSON includes a recent `updated` ISO date, and `trains` array with train `id`, `observedAt` ISO date, optional `cancelled`, and `viewpoints` mapping such as `{"glenfinnan":{"etaMinutes":8}}`.

The app deliberately will **not** fabricate a position, ETA, alert or delay if data is unavailable. National Rail Darwin does not provide continuous GPS positions; estimates derived from railway event information must be labelled as such.

## Security and privacy

- All VAPID private keys and railway provider tokens reside in server environment variables.
- Browser geolocation is not sent to this push backend.
- Subscriptions are stored in Netlify Blobs; endpoints are secret bearer URLs and should not be logged.
- The scheduled function checks current data and sends at most one alert per train, viewpoint, lead time and UTC calendar day to each subscriber. Expired push endpoints are deleted.
- Do not claim guaranteed delivery: iOS and browser notification settings, power-saving, reception and push provider conditions can affect delivery.
