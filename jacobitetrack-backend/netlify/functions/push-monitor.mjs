import webpush from "web-push";
import {getStore} from "@netlify/blobs";
const env=key=>Netlify.env.get(key)||"";
const fresh=s=>typeof s==="string"&&Number.isFinite(Date.parse(s))&&Date.now()-Date.parse(s)<600000&&Date.parse(s)<Date.now()+60000;
export default async ()=>{
 const url=env("RAIL_FEED_URL"),pub=env("VAPID_PUBLIC_KEY"),priv=env("VAPID_PRIVATE_KEY"),contact=env("VAPID_CONTACT");
 if(!url||!pub||!priv||!contact)return;
 let feed;
 try{
  if(new URL(url).protocol!=="https:")throw Error("Rail feed must use HTTPS");
  const r=await fetch(url,{headers:env("RAIL_FEED_TOKEN")?{Authorization:"Bearer "+env("RAIL_FEED_TOKEN")}:{},signal:AbortSignal.timeout(8000)});
  if(!r.ok)throw Error("Rail feed "+r.status);
  feed=await r.json();if(!fresh(feed.updated)||!Array.isArray(feed.trains))throw Error("Stale feed");
 }catch(err){console.log("No railway alerts:",err.message);return}
 const trains=feed.trains.filter(t=>typeof t.id==="string"&&fresh(t.observedAt)&&!t.cancelled);
 webpush.setVapidDetails(contact,pub,priv);
 const db=getStore("jacobitetrack-alert-subscriptions"),all=await db.list({prefix:"sub/"});
 for(const item of all.blobs.slice(0,100)){
  const record=await db.get(item.key,{type:"json"});if(!record?.subscription)continue;
  const due=trains.filter(t=>Number.isFinite(t.viewpoints?.[record.viewpoint]?.etaMinutes)&&t.viewpoints[record.viewpoint].etaMinutes>=0&&t.viewpoints[record.viewpoint].etaMinutes<=record.leadMinutes).sort((a,b)=>a.viewpoints[record.viewpoint].etaMinutes-b.viewpoints[record.viewpoint].etaMinutes);
  if(!due.length)continue;
  const train=due[0],event=train.id+"|"+record.viewpoint+"|"+new Date().toISOString().slice(0,10)+"|"+record.leadMinutes;
  if(record.lastSent?.[event])continue;
  const m=Math.ceil(train.viewpoints[record.viewpoint].etaMinutes);
  try{
   await webpush.sendNotification(record.subscription,JSON.stringify({title:"Train approaching "+record.viewpoint,body:train.id+" expected in "+(m<1?"less than 1 minute":m+" minutes")+". Railway-data estimate.",tag:event}),{TTL:120});
   const lastSent={...(record.lastSent||{}),[event]:Date.now()};
   const cutoff=Date.now()-259200000;for(const [key,val] of Object.entries(lastSent))if(val<cutoff)delete lastSent[key];
   await db.setJSON(item.key,{...record,lastSent});
  }catch(err){if(err.statusCode===404||err.statusCode===410)await db.delete(item.key);else console.log("Send failed:",err.statusCode||err.message)}
 }
};
export const config={schedule:"*/1 * * * *"};
