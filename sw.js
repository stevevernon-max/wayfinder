const CACHE='wayfinder-shell-v36';
const FILES=['./','./index.html','./styles.css?v=36','./app.js?v=36','./tomtom-config.js?v=35','./manifest.webmanifest','./apple-touch-icon.png','./highland-road-192.png','./highland-road-512.png'];
self.addEventListener('install',event=>event.waitUntil(caches.open(CACHE).then(c=>c.addAll(FILES)).then(()=>self.skipWaiting())));
self.addEventListener('activate',event=>event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',event=>{const req=event.request;if(req.method!=='GET'||new URL(req.url).origin!==self.location.origin)return;if(req.mode==='navigate'){event.respondWith(fetch(req).catch(()=>caches.match('./index.html')));return;}event.respondWith(caches.match(req).then(hit=>hit||fetch(req).then(res=>{if(res.ok){const copy=res.clone();caches.open(CACHE).then(c=>c.put(req,copy))}return res})))});
