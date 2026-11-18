import {getStore} from "@netlify/blobs";
import {createHash} from "node:crypto";
const ORIGIN="https://stevevernon-max.github.io";
const env=(key)=>Netlify.env.get(key)||"";
const store=()=>getStore("jacobitetrack-alert-subscriptions");
const hash=e=>createHash("sha256").update(e).digest("hex");
const reply=(value,status=200)=>Response.json(value,{status,headers:{"Access-Control-Allow-Origin":ORIGIN,"Access-Control-Allow-Methods":"GET,POST,OPTIONS","Access-Control-Allow-Headers":"Content-Type","Cache-Control":"no-store"}});
const ready=()=>Boolean(env("VAPID_PUBLIC_KEY")&&env("VAPID_PRIVATE_KEY")&&env("VAPID_CONTACT")&&env("RAIL_FEED_URL"));
function valid(sub){try{
 const u=new URL(sub.endpoint),h=u.hostname;
 return u.protocol==="https:"&&(h==="web.push.apple.com"||h.endsWith(".push.apple.com")||h==="fcm.googleapis.com"||h==="updates.push.services.mozilla.com"||h.endsWith(".notify.windows.com"))&&sub.endpoint.length<2048&&typeof sub.keys?.p256dh==="string"&&typeof sub.keys?.auth==="string";
}catch{return false}}
export default async (req)=>{
 const path=new URL(req.url).pathname;
 if(req.method==="OPTIONS")return reply({},200);
 if(path==="/api/push/config"&&req.method==="GET")return reply({ready:ready(),publicKey:ready()?env("VAPID_PUBLIC_KEY"):""});
 if(req.method!=="POST"||req.headers.get("origin")!==ORIGIN)return reply({error:"Forbidden"},403);
 if(Number(req.headers.get("content-length")||0)>8000)return reply({error:"Too large"},413);
 let data;try{data=await req.json()}catch{return reply({error:"Invalid JSON"},400)}
 if(path==="/api/push/subscribe"){
  if(!ready())return reply({error:"Backend or railway feed not ready"},503);
  if(!valid(data.subscription)||!["glenfinnan","lochailort","arisaig","morar"].includes(data.viewpoint)||![5,10,15,20].includes(data.leadMinutes))return reply({error:"Invalid alert preferences"},400);
  await store().setJSON("sub/"+hash(data.subscription.endpoint),{subscription:data.subscription,viewpoint:data.viewpoint,leadMinutes:data.leadMinutes,lastSent:{}});
  return reply({subscribed:true},201);
 }
 if(path==="/api/push/unsubscribe"){
  if(typeof data.endpoint!=="string"||data.endpoint.length>2048)return reply({error:"Invalid endpoint"},400);
  await store().delete("sub/"+hash(data.endpoint));return reply({unsubscribed:true});
 }
 return reply({error:"Not found"},404);
};
export const config={path:["/api/push/config","/api/push/subscribe","/api/push/unsubscribe"]};
