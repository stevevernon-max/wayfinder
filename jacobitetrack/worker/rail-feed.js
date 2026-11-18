/**
 * JacobiteTrack railway-feed adapter — deploy as Cloudflare Worker.
 *
 * Environment variables:
 *   RAIL_SOURCE_URL: an authorised, HTTPS railway-data endpoint
 *   RAIL_SOURCE_TOKEN: optional bearer token for that endpoint (secret)
 *
 * This adapter expects the upstream railway feed (or your own integration)
 * to return an array of train progress reports:
 * { trains:[{id, direction:"out"|"back", progress:0..100, observedAt:ISO}],
 *   updated:ISO }
 * It does NOT produce positions from a timetable and does NOT include a
 * live Signalbox/Trainline integration. Only use with permitted data.
 */
export default {
 async fetch(request,env){
  const url=new URL(request.url);
  if(request.method!=="GET"||url.pathname!=="/trains")return new Response("Not found",{status:404});
  const headers={"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store","Access-Control-Allow-Origin":"*"};
  if(!env.RAIL_SOURCE_URL)return Response.json({trains:[],updated:new Date().toISOString(),status:"not_configured",message:"No authorised railway feed configured"},{headers,status:503});
  try{
    const target=new URL(env.RAIL_SOURCE_URL);
    if(target.protocol!=="https:")throw Error("Only HTTPS sources allowed");
    const upstream=await fetch(target.toString(),{headers:env.RAIL_SOURCE_TOKEN?{Authorization:"Bearer "+env.RAIL_SOURCE_TOKEN}:{}});
    if(!upstream.ok)throw Error("Upstream HTTP "+upstream.status);
    const data=await upstream.json();
    if(!Array.isArray(data.trains))throw Error("Expected trains array");
    const trains=data.trains.filter(t=>Number.isFinite(t.progress)&&t.progress>=0&&t.progress<=100&&["out","back"].includes(t.direction)&&typeof t.observedAt==="string"&&Number.isFinite(Date.parse(t.observedAt))&&Date.now()-Date.parse(t.observedAt)<600000).map(t=>({id:String(t.id).slice(0,80),progress:t.progress,direction:t.direction,observedAt:t.observedAt,delayMinutes:Number.isFinite(t.delayMinutes)?Math.max(0,Math.min(1440,t.delayMinutes)):null,cancelled:t.cancelled===true,quality:String(t.quality||"Railway movement estimate").slice(0,80),viewpoints:Object.fromEntries(Object.entries(t.viewpoints||{}).filter(([id,v])=>["glenfinnan","lochailort","arisaig","morar"].includes(id)&&v&&Number.isFinite(v.etaMinutes)&&v.etaMinutes>=0&&v.etaMinutes<600).map(([id,v])=>[id,{etaMinutes:v.etaMinutes}]))}));
    return Response.json({trains,updated:new Date().toISOString(),source:"licensed train movement feed",quality:"observed or estimated per upstream",status:"ok"},{headers});
  }catch(e){return Response.json({trains:[],updated:new Date().toISOString(),status:"error",message:"Rail feed unavailable"},{headers,status:502})}
 }
};
