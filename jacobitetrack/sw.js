/* JacobiteTrack service worker — scoped to /wayfinder/jacobitetrack/ */
const APP="./";
self.addEventListener("install",()=>{self.skipWaiting()});
self.addEventListener("activate",event=>{event.waitUntil(self.clients.claim())});
self.addEventListener("push",event=>{
 let data={title:"JacobiteTrack",body:"Train alert received",url:APP};
 try{if(event.data){const value=event.data.json();if(value&&typeof value==="object")data={...data,...value}}}catch{if(event.data)data.body=event.data.text()}
 const title=String(data.title||"JacobiteTrack").slice(0,90);
 const options={body:String(data.body||"").slice(0,240),icon:"./icon.svg",badge:"./icon.svg",tag:String(data.tag||"train-alert").slice(0,110),data:{url:APP},renotify:true};
 event.waitUntil(self.registration.showNotification(title,options));
});
self.addEventListener("notificationclick",event=>{
 event.notification.close();
 event.waitUntil((async()=>{
  const windows=await self.clients.matchAll({type:"window",includeUncontrolled:true});
  for(const c of windows){if(c.url.includes("/jacobitetrack/")){await c.focus();return}}
  await self.clients.openWindow(new URL(APP,self.registration.scope).href);
 })());
});
