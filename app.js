(() => {
  'use strict';
  const KEY='wayfinder.v1', TRIPS='wayfinder.trips.v1';
  const homeAddress='An Sabhol, Roshven, PH38 4NB';
  const homeCoordinate='56.842778,-5.748333';
  const defaults={start:'Home',destination:'',arrive:'15:00',homeAddress,provider:'tomtom',adapterUrl:'',stops:[]};
  let state={...defaults};
  try { state={...defaults,...JSON.parse(localStorage.getItem(KEY)||'{}'),homeAddress,provider:'tomtom'}; } catch {}
  delete state.apiKey;
  if(state.homeCoordinate!==homeCoordinate){
    delete state.cachedRoute;
    delete state.journeyTracking;
    state.homeCoordinate=homeCoordinate;
  }
  let trips=[]; try { trips=JSON.parse(localStorage.getItem(TRIPS)||'[]'); if(!Array.isArray(trips)) trips=[]; } catch {}
  const $=id=>document.getElementById(id);
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const n=v=>Math.max(0,Math.floor(Number(v)||0));
  const mins=val=>{const [h,m]=String(val||'0:0').split(':').map(Number);return (h||0)*60+(m||0)};
  const clock=m=>{m=(m%1440+1440)%1440;return `${String(Math.floor(m/60)).padStart(2,'0')}:${String(m%60).padStart(2,'0')}`};
  const duration=m=>`${Math.floor(m/60)?`${Math.floor(m/60)}h `:''}${m%60}m`;
  const persist=()=>{try{localStorage.setItem(KEY,JSON.stringify(state))}catch{status('Could not save settings on this device. Check available storage.')}};
  const status=s=>{$('status').textContent=s};
  const loc=v=>/^home$/i.test(String(v||'').trim())?(state.homeAddress||'Home'):String(v||'').trim();
  const shortPlace=value=>/^Current Location \[/.test(String(value))?'Current Location':String(value||'').split(',').slice(0,2).join(',').trim();
  const localStores=[
    {match:/^mowi(?: scotland)?$/i,name:'Mowi Scotland - Lochailort',address:'Mowi Scotland - Lochailort, Inverness-shire, PH38 4LZ'},
    {match:/^morrisons(?:\s+fort(?:\s+william)?)?$/i,name:'Morrisons Fort William',address:'An Aird Road, Fort William, PH33 6AN'},
    {match:/^(?:m\s*&\s*s|marks and spencer)(?:\s+fort(?:\s+william)?)?$/i,name:'M&S Fort William',address:'Fort William Retail Park, North Road, Fort William'}
  ];
  const preferredStore=query=>localStores.find(store=>store.match.test(query));
  const mapsUrl=(place,coord)=>'https://maps.apple.com/?'+(coord
    ? 'll='+encodeURIComponent(coord)+'&q='+encodeURIComponent(shortPlace(place))
    : 'q='+encodeURIComponent(place));
  let routeData=state.cachedRoute, timelineEvents=[], gpsFix, gpsWatch, arrivalMargin=null;
  function newJourneyTracking(signature=''){
    return {version:2,signature,leftAt:null,origin:null,departureCandidateAt:null};
  }
  const savedTracking=state.journeyTracking;
  let departureTracking=savedTracking?.version===2 && savedTracking.leftAt
    && (!savedTracking.arrivedAt || Date.now()-savedTracking.arrivedAt<3600000)
    ? savedTracking : newJourneyTracking();
  function trackDeparture(position){
    const route=currentRoute();
    if(!route || Date.now()-position.timestamp>30000 || position.coords.accuracy>30)return;
    if(departureTracking.signature!==route.signature){
      if(departureTracking.leftAt){
        departureTracking.signature=route.signature;
        departureTracking.liveLeg=0;
      }else departureTracking=newJourneyTracking(route.signature);
    }
    const fix=position.coords;
    const distanceTo=coordinate=>{
      const [latitude,longitude]=coordinate.split(',').map(Number);
      return 111000*Math.hypot(fix.latitude-latitude,
        (fix.longitude-longitude)*Math.cos(latitude*Math.PI/180));
    };
    const startDistance=distanceTo(route.coords[0]);
    if(departureTracking.leftAt){
      if(!departureTracking.arrivedAt && distanceTo(route.coords.at(-1))+fix.accuracy<=120){
        departureTracking.arrivedAt=position.timestamp;
        departureTracking.completedMargin=departureTracking.targetArrival
          ? Math.round((departureTracking.targetArrival-position.timestamp)/60000) : null;
      }
      return;
    }
    // Capture the phone's position near Start so geocoding offsets cannot trigger departure.
    if(!departureTracking.origin){
      if(startDistance<=150){
        departureTracking.origin=fix.latitude+','+fix.longitude;
        departureTracking.originAt=position.timestamp;
      }
      return;
    }
    if(distanceTo(departureTracking.origin)-fix.accuracy<50){
      departureTracking.departureCandidateAt=null;
      return;
    }
    if(!departureTracking.departureCandidateAt){
      departureTracking.departureCandidateAt=position.timestamp;
      return;
    }
    if(position.timestamp-departureTracking.departureCandidateAt<10000)return;
    departureTracking.leftAt=position.timestamp;
    departureTracking.targetArrival=departure().getTime()+total()*60000;
  }

  const weather=new Map();
  const total=()=>state.stops.reduce((a,s)=>a+n(s.drive)+n(s.stay),0)+n(state.destinationDrive);
  function departure(){const now=new Date(),d=new Date();d.setHours(0,0,0,0);d.setMinutes(mins(state.arrive));if(d.getTime()<=now.getTime())d.setDate(d.getDate()+1);return new Date(d.getTime()-total()*60000)}
  function renderStops(){ $('stops').innerHTML=(state.stops.length ? '<div class="stop-head" aria-hidden="true"><span></span><span>Stops</span><span>Drive</span><span>Stay</span></div>' : '')+state.stops.map((s,i)=>`<div class="stop-row" data-i="${i}"><button type="button" class="reorder" data-reorder="${i}" aria-label="Reorder stop ${i+1}. Drag or use the up and down arrow keys." title="Drag to reorder">≡</button><div class="stop-place"><a class="map-link" data-map="${i}" aria-label="Open stop ${i+1} in Apple Maps"></a><input autocomplete="off" data-field="name" aria-label="Stop ${i+1}" placeholder="Stop ${i+1}" value="${esc(shortPlace(s.name))}"></div><input class="stop-time" data-field="drive" aria-label="Drive minutes to stop ${i+1}" type="number" min="0" inputmode="numeric" placeholder="drive" value="${esc(s.drive)}"><select class="stop-time stay-time" data-field="stay" aria-label="Stay minutes at stop ${i+1}">${Array.from({length:61},(_,k)=>k*5).map(v=>`<option value="${v}" ${n(s.stay)===v?'selected':''}>${v}</option>`).join('')}</select><button class="remove" data-remove="${i}" aria-label="Remove stop">×</button></div>`).join(''); }
  function render(){
    closeLookup();
    $('start').value=shortPlace(state.start);
    $('destination').value=shortPlace(state.destination);
    $('destination-drive').value=state.destinationDrive ?? '';
    renderStops();
    renderTrips();
    updatePlan();
  }
  function updatePlan(){
    state.journeyTracking=departureTracking;
    $('destination-clear').hidden=!$('destination').value;
    const driving=state.stops.reduce((sum, stop)=>sum+n(stop.drive), n(state.destinationDrive));
    const staying=total()-driving;
    const [hour, minute]=state.arrive.split(':');
    $('arrive-open').innerHTML='Arrive by <b>'+esc(hour)+':'+esc(minute)+'</b>';
    $('arrive-open').setAttribute('aria-label', 'Arrive by '+state.arrive+', change time');
    $('total-time').textContent=total()?duration(total()):'—';
    $('drive-time').textContent=duration(driving);
    $('stay-time').textContent=duration(staying);
    renderTimeline();
    setMaps();
    persist();
    tick();
  }
  function renderTimeline(){
    if(!state.destination.trim()){ timelineEvents=[]; $('journey-stop-markers').replaceChildren(); $('timeline').innerHTML='<div class="empty">Add a destination to see your plan.</div>'; return; }
    const start=departureTracking.targetArrival && departureTracking.signature===JSON.stringify(routeNames()) ? departureTracking.targetArrival-total()*60000 : departure().getTime();
    let time=start;
    const names=routeNames();
    const route=currentRoute();
    const events=[{time,title:shortPlace(state.start||'Home').split(',')[0],location:0,forecastTime:start}];
    state.stops.forEach((stop,index)=>{
      time+=n(stop.drive)*60000;
      const arrival=time;
      time+=n(stop.stay)*60000;
      events.push({time,forecastTime:arrival,title:(stop.name||'Stop').split(',')[0].trim(),range:true,
        location:index+1,facility:stop.facility,delay:route?.delays[index],note:n(stop.stay)?n(stop.stay)+' min stay':''});
    });
    time+=n(state.destinationDrive)*60000;
    events.push({time,forecastTime:time,title:(state.destination).split(',')[0].trim(),
      location:names.length-1,delay:route?.delays.at(-1)});
    timelineEvents=events;
    const span=events.at(-1).time-start;
    $('journey-stop-markers').innerHTML=span>0 ? events.slice(1,-1).map((stop,i)=>
      '<span style="left:'+Math.min(97,Math.max(3,100*(stop.forecastTime-start)/span))+'%" title="'+esc(stop.title)+'">'+(i+1)+'</span>'
    ).join('') : '';
    $('timeline').innerHTML=events.map(event=>{
      const coord=route?.coords[event.location] || selectedPlaces.get(names[event.location]);
      const hourly=weather.get(coord);
      const hour=Math.ceil(event.forecastTime/3600000)*3600;
      const probability=hourly?.precipitation_probability[hourly.time.indexOf(hour)];
      const rain=Number.isFinite(probability)?'<span class="rain" title="Hourly precipitation probability">☂ '+probability+'%</span>':'';
      const timeText=stamp=>{
        const date=new Date(stamp);
        return clock(date.getHours()*60+date.getMinutes());
      };
      const range=event.range ? timeText(event.forecastTime)+'–'+timeText(event.time) : timeText(event.time);
      const delayReason=route?.reasons[event.location-1]||'Reason unavailable';
      const delay=event.delay>0?' <span class="inline-delay" aria-label="'+esc(delayReason+', '+event.delay+' minutes included')+'">+'+event.delay+'</span>':'';
      const details=event.note?'<small>'+esc(event.note)+'</small>':'';
      const facility=event.facility || (/\b(public toilets?|restrooms?)\b/i.test(event.title)?'toilet':/\b(charging station|ev charger|supercharger)\b/i.test(event.title)?'charger':'');
      const marker=facility==='toilet' ? $('toilet-lookup').innerHTML : facility==='charger' ? '⚡' : '';
      const track='<span class="track'+(marker?' facility-track':'')+'">'+(marker?'<span class="facility-marker" role="img" aria-label="'+(facility==='toilet'?'Public toilet':'EV charger')+'">'+marker+'</span>':'')+'</span>';
      return '<div class="event"><time>'+range+'</time>'+track+'<div>'
        +rain+'<b>'+esc(event.title)+delay+'</b>'+details+'</div></div>';
    }).join('');
    drawProgress();
  }
  function routeNames(){
    return [state.start,...state.stops.map(stop=>stop.name),state.destination].map(value=>{
      const name=loc(value);
      return preferredStore(name)?.address||name;
    });
  }
  function currentRoute(){
    return routeData?.signature===JSON.stringify(routeNames()) ? routeData : null;
  }

  async function loadWeather(coords){
    const unique=[...new Set(coords.filter(Boolean))];
    if(!unique.length)return;
    const points=unique.map(coord=>coord.split(','));
    const params=new URLSearchParams({
      latitude:points.map(point=>point[0]).join(','),
      longitude:points.map(point=>point[1]).join(','),
      hourly:'precipitation_probability', timeformat:'unixtime', past_days:'1', forecast_days:'3'
    });
    try{
      const response=await fetch('https://api.open-meteo.com/v1/forecast?'+params,{signal:AbortSignal.timeout(12000)});
      if(!response.ok)return;
      const body=await response.json();
      const forecasts=Array.isArray(body)?body:[body];
      forecasts.forEach((forecast,index)=>{
        if(forecast.hourly?.time && forecast.hourly?.precipitation_probability)weather.set(unique[index],forecast.hourly);
      });
      renderTimeline();
    }catch{ /* Forecasts are optional when offline or unavailable. */ }
  }

  // Match GPS to the closest point on the road geometry for the scheduled leg.
  function roadProgress(points,fix){
    if(!points || points.length<2)return null;
    const scale=Math.cos(fix.latitude*Math.PI/180);
    let totalDistance=0,bestDistance=Infinity,bestAlong=0;
    for(let i=1;i<points.length;i++){
      const a=points[i-1],b=points[i];
      const x=(b.longitude-a.longitude)*scale,y=b.latitude-a.latitude;
      const length=Math.hypot(x,y);
      if(!length)continue;
      const px=(fix.longitude-a.longitude)*scale,py=fix.latitude-a.latitude;
      const fraction=Math.max(0,Math.min(1,(px*x+py*y)/(length*length)));
      const distance=Math.hypot(px-fraction*x,py-fraction*y);
      if(distance<bestDistance){bestDistance=distance;bestAlong=totalDistance+fraction*length;}
      totalDistance+=length;
    }
    const distance=bestDistance*111000;
    return totalDistance
      ? {progress:bestAlong/totalDistance,distance} : null;
  }
  let liveEstimate, lastLiveRequest, liveRequestPending=false;
  function refreshLiveEstimate(route,leg,position){
    const fix=position.coords;
    const now=Date.now();
    const previous=lastLiveRequest;
    if(liveRequestPending || (previous?.route===route && now-previous.time<60000))return;
    if(previous?.route===route && previous.leg===leg && now-previous.time<180000
      && 111000*Math.hypot(fix.latitude-previous.latitude,
        (fix.longitude-previous.longitude)*Math.cos(fix.latitude*Math.PI/180))<500)return;
    const tracking=departureTracking;
    lastLiveRequest={route,leg,time:now,latitude:fix.latitude,longitude:fix.longitude};
    liveRequestPending=true;
    const coords=[fix.latitude+','+fix.longitude,...route.coords.slice(leg+1)];
    void (async ()=>{
      try{
        const response=await tomTom({coords,chargerDrive:true});
        if(!response.ok)return;
        const seconds=(await response.json()).routes?.[0]?.summary?.travelTimeInSeconds;
        if(Number.isFinite(seconds) && currentRoute()===route && departureTracking===tracking
          && !tracking.arrivedAt)liveEstimate={route,tracking,leg,time:now,seconds};
      }catch{ /* Keep the estimate from the planned route while live routing is unavailable. */ }
      finally{liveRequestPending=false;}
    })();
  }

  function drawProgress(){
    const rows=$('timeline').querySelectorAll('.event');
    const now=Date.now();
    const route=currentRoute();
    const tracking=route && departureTracking.signature===route.signature && departureTracking.leftAt && !departureTracking.arrivedAt;
    const fresh=gpsFix && now-gpsFix.timestamp<30000 && gpsFix.coords.accuracy<=65;
    const previous=departureTracking.liveLeg||0;
    let best;
    let liveEta=null;
    arrivalMargin=null;
    if(tracking && fresh){
      const candidates=route.paths.map((points,i)=>{
        const match=roadProgress(points,gpsFix.coords);
        return match && {i,...match};
      }).filter(match=>match && match.i>=previous).sort((a,b)=>a.distance-b.distance);
      const closest=candidates[0];
      const current=candidates.find(match=>match.i===previous);
      best=current && closest && current.distance<=closest.distance+Math.max(25,gpsFix.coords.accuracy)
        ? current : closest;
      const nearby=best && best.distance<=Math.max(120,gpsFix.coords.accuracy*2);
      // Keep the next planned stop when off route until GPS clearly rejoins a later leg.
      if(!nearby)best=current||best;
      if(best && timelineEvents[best.i+1]){
        const {i,progress}=best;
        if(nearby)departureTracking.liveLeg=i;
        const next=timelineEvents[i+1];
        const drive=n(i<state.stops.length?state.stops[i].drive:state.destinationDrive)*60000;
        const stays=state.stops.slice(i).reduce((sum,stop)=>sum+n(stop.stay),0)*60000;
        const plannedRemaining=Math.max(0,timelineEvents.at(-1).time-next.forecastTime)+(1-progress)*drive;
        const rejoinTime=nearby?0:best.distance/8*1000;
        const estimate=liveEstimate;
        const routed=!nearby && estimate?.route===route && estimate.tracking===departureTracking
          && estimate.leg===i && now-estimate.time<180000;
        liveEta=now+(routed?estimate.seconds*1000+stays:plannedRemaining+rejoinTime);
        const target=departureTracking.targetArrival||timelineEvents.at(-1).time;
        arrivalMargin=Math.round((target-liveEta)/60000);
        $('gps-status').textContent=nearby ? Math.round(progress*100)+'% of current leg'
          : routed ? 'Off route · ETA via remaining stops' : 'Off route · approximate ETA while checking roads';
        if(!nearby)refreshLiveEstimate(route,i,gpsFix);
        $('live-stop').textContent=(i<state.stops.length?'Next stop · ':'Destination · ')+next.title.replace(/^End · /,'');
        const start=timelineEvents[0].time;
        const span=timelineEvents.at(-1).time-start;
        const elapsed=timelineEvents[i].time-start+progress*drive;
        $('live-progress').value=span>0?Math.max(0,Math.min(1,elapsed/span)):0;
      }
    }
    if(liveEta===null){
      best=undefined;
      if(gpsFix)$('gps-status').textContent=departureTracking.arrivedAt && departureTracking.signature===route?.signature
        ? 'Destination reached.'
        : !route ? 'Route changed. Tap Get travel times to refresh the ETA.'
        : !fresh ? 'Waiting for a fresh, accurate GPS reading.'
        : tracking ? 'Route geometry unavailable. Tap Get travel times.'
        : departureTracking.origin ? 'GPS ready. Waiting for departure.' : 'Waiting for an accurate GPS reading near Start.';
      $('live-stop').textContent='Waiting for live progress';
      $('live-progress').value=0;
    }
    const signed=arrivalMargin===null?'—':(arrivalMargin>=0?'+':'−')+Math.abs(arrivalMargin)+' min';
    $('live-margin').textContent=signed;
    $('live-eta').textContent=liveEta===null?'—':new Date(liveEta).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});
    rows.forEach((row,i)=>{
      const track=row.querySelector('.track');
      if(best?.i===i && rows[i+1]){
        track.dataset.progress='on-time';
        row.dataset.tracking='on-time';
        const height=rows[i+1].offsetTop-row.offsetTop;
        row.style.setProperty('--progress-y',(8+best.progress*height)+'px');
      }else{delete track.dataset.progress;delete row.dataset.tracking;}
    });
  }

  function setMaps(){
    const names=routeNames();
    const route=currentRoute();
    document.querySelectorAll('[data-map]').forEach(link=>{
      const field=link.dataset.map;
      const index=field==='start'?0:field==='destination'?names.length-1:Number(field)+1;
      const place=names[index];
      if(place && place!=='Home'){
        link.href=mapsUrl(place,route?.coords[index]||selectedPlaces.get(place));
        link.target='_blank';
        link.rel='noopener';
      }else link.removeAttribute('href');
    });
  }
  function renderTrips(){ $('saved').innerHTML=trips.length?trips.map((t,i)=>`<button class="trip-chip" data-load="${i}">${esc(t.name)}<span data-delete="${i}" aria-label="Delete">×</span></button>`).join(''):'<span class="empty">Saved journeys appear here.</span>'; }
  function tick(){
    const now=Date.now();
    if(departureTracking.arrivedAt && now-departureTracking.arrivedAt>=3600000){
      departureTracking=newJourneyTracking();
      state.journeyTracking=departureTracking;
      persist();
      renderTimeline();
    }
    drawProgress();
    const underway=Boolean(departureTracking.leftAt);
    const arrived=departureTracking.arrivedAt;
    $('planned-status').hidden=underway;
    $('live-journey').hidden=!underway;
    $('live-margin').hidden=!underway;
    $('margin-label').hidden=!underway;
    $('countdown').hidden=underway;
    $('count-label').hidden=underway;
    $('journey-phase').textContent=arrived?'Journey complete':underway?'Journey underway':'Journey planned';
    $('journey-phase').classList.toggle('is-live',underway && !arrived);
    $('live-eta-label').textContent=arrived?'Arrived':'ETA';
    if(arrived){
      $('live-stop').textContent=shortPlace(state.destination);
      $('live-eta').textContent=clock(new Date(arrived).getHours()*60+new Date(arrived).getMinutes());
      $('live-progress').value=1;
      arrivalMargin=departureTracking.completedMargin ?? null;
      $('live-margin').textContent=arrivalMargin===null?'—':(arrivalMargin>=0?'+':'−')+Math.abs(arrivalMargin)+' min';
    }
    $('live-margin').dataset.tone=arrivalMargin===null?'unknown':arrivalMargin<0?'late':'early';
    $('gps-reading').textContent=gpsFix ? 'GPS ±'+Math.round(gpsFix.coords.accuracy)+' m' : 'No GPS reading';
    const leave=departure();
    const seconds=Math.ceil(Math.abs(leave.getTime()-now)/1000);
    const late=!underway && total()>0 && leave.getTime()<now;
    $('timeline').classList.toggle('departure-overdue',late);
    $('count-label').textContent=late?'LATE BY':'SET OFF IN';
    $('leave-time').dataset.tone=late?'late':total()?'early':'unknown';
    $('gps-reading').dataset.signal=gpsFix && now-gpsFix.timestamp<30000 && gpsFix.coords.accuracy<=65?'ok':'issue';
    $('leave-time').textContent=total()?clock(leave.getHours()*60+leave.getMinutes()):'—:—';
    $('countdown').textContent=total()
      ? `${Math.floor(seconds/3600)}h ${Math.floor(seconds%3600/60)}m ${String(seconds%60).padStart(2,'0')}s`
      : '—';
  }

  for(const field of ['start','destination']){
    $(field).addEventListener('input', event=>{
      state[field]=event.target.value;
      updatePlan();
    });
  }
  $('destination-clear').addEventListener('click', ()=>{
    const destination=$('destination');
    routeRequest++;
    routeData=undefined;
    $('calculate').disabled=false;
    state.destinationDrive='';
    $('destination-drive').value='';
    destination.value='';
    destination.dispatchEvent(new Event('input',{bubbles:true}));
    destination.focus();
    status('');
  });
  $('destination-drive').addEventListener('input', event=>{
    state.destinationDrive=event.target.value;
    updatePlan();
  });
  $('stops').addEventListener('input', event=>{
    const row=event.target.closest('[data-i]');
    const field=event.target.dataset.field;
    if(!row || !field)return;
    state.stops[Number(row.dataset.i)][field]=event.target.value;
    updatePlan();
  });

  const arrivalDialog=$('arrival-dialog');
  const wheelSize=38;
  function setupWheel(id,length){
    const wheel=$(id);
    const value=()=>Math.max(0,Math.min(length-1,Math.round(wheel.scrollTop/wheelSize)));
    const showValue=()=>wheel.setAttribute('aria-valuenow',String(value()));
    for(let i=0;i<length;i++){
      const item=document.createElement('div');
      item.textContent=String(i).padStart(2,'0');
      item.setAttribute('aria-hidden','true');
      item.onclick=()=>{wheel.scrollTop=i*wheelSize;showValue();};
      wheel.append(item);
    }
    wheel.addEventListener('scroll',showValue);
    wheel.addEventListener('keydown',event=>{
      if(!['ArrowUp','ArrowDown','Home','End'].includes(event.key))return;
      event.preventDefault();
      const next=event.key==='Home'?0:event.key==='End'?length-1:value()+(event.key==='ArrowDown'?1:-1);
      wheel.scrollTop=next*wheelSize;
      showValue();
    });
    return {value,select(index){wheel.scrollTop=index*wheelSize;showValue();}};
  }
  const hourWheel=setupWheel('arrival-hour',24);
  const minuteWheel=setupWheel('arrival-minute',60);
  $('arrive-open').onclick=()=>{
    closeLookup();
    const [hour,minute]=state.arrive.split(':').map(Number);
    arrivalDialog.returnValue='';
    arrivalDialog.showModal();
    hourWheel.select(hour);
    minuteWheel.select(minute);
  };
  arrivalDialog.addEventListener('submit',event=>{
    if(event.submitter?.value!=='done')return;
    state.arrive=[hourWheel.value(),minuteWheel.value()].map(value=>String(value).padStart(2,'0')).join(':');
    if(departureTracking.leftAt && !departureTracking.arrivedAt){
      const target=new Date(departureTracking.targetArrival||Date.now());
      target.setHours(Math.floor(mins(state.arrive)/60),mins(state.arrive)%60,0,0);
      departureTracking.targetArrival=target.getTime();
    }
    updatePlan();
  });
  $('stops').addEventListener('click',e=>{if(e.target.dataset.remove!==undefined){state.stops.splice(+e.target.dataset.remove,1);render()}});
  function moveStop(from,to){
    if(from===to || to<0 || to>=state.stops.length)return;
    const [stop]=state.stops.splice(from,1);
    state.stops.splice(to,0,stop);
    // Drive times are for the incoming leg, so only keep unchanged legs.
    const previous=[...state.stops];
    previous.splice(to,1);
    previous.splice(from,0,stop);
    state.stops.forEach((item,i)=>{
      const oldIndex=previous.indexOf(item);
      if(previous[oldIndex-1]!==state.stops[i-1])item.drive='';
    });
    if(previous.at(-1)!==state.stops.at(-1))state.destinationDrive='';
    routeData=undefined;
    render();
    $('stops').querySelector('[data-reorder="'+to+'"]')?.focus({preventScroll:true});
    if(!routeNames().some(name=>!name || name==='Home'))void calcRoute();
    else status('Stops reordered. Enter new drive times for the changed legs.');
  }
  let stopDrag;
  $('stops').addEventListener('pointerdown',event=>{
    const handle=event.target.closest('[data-reorder]');
    if(!handle || event.button!==0)return;
    const index=Number(handle.dataset.reorder);
    stopDrag={pointer:event.pointerId,from:index,to:index};
    handle.setPointerCapture(event.pointerId);
  });
  $('stops').addEventListener('pointermove',event=>{
    if(stopDrag?.pointer!==event.pointerId)return;
    const target=document.elementFromPoint(event.clientX,event.clientY)?.closest('.stop-row');
    if(target)stopDrag.to=Number(target.dataset.i);
    $('stops').querySelectorAll('.stop-row').forEach(row=>{
      row.classList.toggle('drop-stop',Number(row.dataset.i)===stopDrag.to && stopDrag.to!==stopDrag.from);
    });
  });
  function finishStopDrag(event){
    if(stopDrag?.pointer!==event.pointerId)return;
    const {from,to}=stopDrag;
    stopDrag=undefined;
    $('stops').querySelector('.drop-stop')?.classList.remove('drop-stop');
    if(event.type==='pointerup')moveStop(from,to);
  }
  $('stops').addEventListener('pointerup',finishStopDrag);
  $('stops').addEventListener('pointercancel',finishStopDrag);
  $('stops').addEventListener('keydown',event=>{
    if(event.target.dataset.reorder===undefined || !['ArrowUp','ArrowDown'].includes(event.key))return;
    event.preventDefault();
    const from=Number(event.target.dataset.reorder);
    moveStop(from,from+(event.key==='ArrowUp'?-1:1));
  });
  $('add-stop').onclick=()=>{state.stops.push({name:'',drive:'',stay:20});render();$('stops').lastElementChild?.querySelector('[data-field=name]')?.focus()};
  $('clear-plan').onclick=()=>{if(!confirm('Clear this journey? Saved trips will stay.'))return;departureTracking=newJourneyTracking();state={...state,journeyTracking:departureTracking,start:state.homeAddress?'Home':'',destination:'',destinationDrive:'',stops:[]};render();status('Journey cleared.')};
  function readImportedTrip(trip){
    const text=value=>typeof value==='string' && value.length<=2000;
    const time=value=>value==='' || ((typeof value==='number' || typeof value==='string') && Number.isFinite(Number(value)) && Number(value)>=0 && Number(value)<=525600);
    if(!trip || !text(trip.name) || !trip.name.trim() || !text(trip.start) || !text(trip.destination)
      || !trip.destination.trim() || typeof trip.arrive!=='string' || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(trip.arrive)
      || !time(trip.destinationDrive??'') || !Array.isArray(trip.stops) || trip.stops.length>100){
      throw Error('This file contains an invalid trip.');
    }
    const stops=trip.stops.map(stop=>{
      if(!stop || !text(stop.name) || !time(stop.drive??'') || !time(stop.stay??'')){
        throw Error('This file contains an invalid stop.');
      }
      const {name,drive='',stay='',facility}=stop;
      return {name,drive,stay,...(['charger','toilet'].includes(facility)?{facility}:{})};
    });
    return {name:trip.name.trim(),start:trip.start,destination:trip.destination,arrive:trip.arrive,stops,destinationDrive:trip.destinationDrive??''};
  }
  const transferStatus=message=>{$('trip-transfer-status').textContent=message};
  $('export-trips').onclick=()=>{
    if(!trips.length){transferStatus('Save a trip before exporting.');return;}
    const file=new Blob([JSON.stringify({format:'wayfinder-trips',version:1,trips})],{type:'application/json'});
    const url=URL.createObjectURL(file);
    const link=document.createElement('a');
    link.href=url;
    link.download='wayfinder-trips.json';
    document.body.append(link);
    link.click();
    link.remove();
    setTimeout(()=>URL.revokeObjectURL(url),30000);
    transferStatus('Exported saved trips. This file contains your trip locations.');
  };
  $('import-trips').onclick=()=>$('trip-file').click();
  $('trip-file').onchange=async event=>{
    const input=event.currentTarget;
    const file=input.files[0];
    input.value='';
    if(!file)return;
    try{
      if(file.size>1000000)throw Error('This trip file is too large.');
      const data=JSON.parse(await file.text());
      if(data?.format!=='wayfinder-trips' || data.version!==1 || !Array.isArray(data.trips)){
        throw Error('Choose a Wayfinder trips export file.');
      }
      const incoming=data.trips.map(readImportedTrip);
      const merged=[...trips];
      const names=new Set(merged.map(trip=>trip.name));
      for(const trip of incoming){
        let name=trip.name;
        for(let copy=2;names.has(name);copy++)name=trip.name+' ('+copy+')';
        merged.push({...trip,name});
        names.add(name);
      }
      localStorage.setItem(TRIPS,JSON.stringify(merged));
      trips=merged;
      renderTrips();
      transferStatus('Imported '+incoming.length+(incoming.length===1?' trip.':' trips.'));
    }catch(error){
      transferStatus(error instanceof SyntaxError?'Choose a Wayfinder trips export file.':error.message);
    }
  };
  $('save-trip').onclick=()=>{if(!state.destination.trim()){status('Add a destination before saving this trip.');return}const name=$('trip-name').value.trim()||`${loc(state.destination)} trip`;trips=trips.filter(t=>t.name!==name.trim());trips.push({name:name.trim(),start:state.start,destination:state.destination,arrive:state.arrive,stops:state.stops.map(s=>({...s})),destinationDrive:state.destinationDrive||''});try{localStorage.setItem(TRIPS,JSON.stringify(trips))}catch{status('Could not save this trip in browser storage.');return}$('trip-name').value='';renderTrips();status(`Saved “${name.trim()}”.`)};
  $('saved').onclick=e=>{const del=e.target.dataset.delete,load=e.target.closest('[data-load]')?.dataset.load;if(del!==undefined){trips.splice(+del,1);localStorage.setItem(TRIPS,JSON.stringify(trips));renderTrips();return}if(load===undefined)return;const t=trips[+load];Object.assign(state,{...t,stops:t.stops.map(stop=>({...stop}))});render();void calcRoute()};
  const dialog=$('settings'); $('settings-open').onclick=()=>openSettings();
  function openSettings(){ $('tomtom-key').value=localStorage.getItem('wayfinder.tomtom-key')||''; $('keep-location').checked=Boolean(state.liveLocation); $('home-address').value=state.homeAddress||'';$('provider').value=state.provider||'manual';$('adapter-url').value=state.adapterUrl||'';dialog.showModal() }
  $('settings-save').onclick=()=>{localStorage.setItem('wayfinder.tomtom-key',$('tomtom-key').value.trim());if($('keep-location').checked){startLocation()}else{stopLocation()}state.adapterUrl=$('adapter-url').value.trim();persist();render();status('Settings saved on this device.');};
  function tomTom(body, signal=AbortSignal.timeout(20000)){
    const key=window.WAYFINDER_TOMTOM_KEY || localStorage.getItem('wayfinder.tomtom-key') || '';
    if(!key)return Promise.resolve(Response.json({error:'Add your TomTom key in Settings.'},{status:503}));
  const coordinate = value => typeof value === 'string' && /^-?\d{1,3}(?:\.\d+)?,-?\d{1,3}(?:\.\d+)?$/.test(value);
  let url;
  if (coordinate(body?.toiletNear)) {
    const [lat, lon] = body.toiletNear.split(',');
    url = new URL('https://api.tomtom.com/search/2/poiSearch/public%20toilet.json');
    url.search = new URLSearchParams({lat, lon, radius:'25000', limit:'12', countrySet:'GB', categorySet:'9932'});
  } else if (coordinate(body?.chargerNear)) {
    const [lat, lon] = body.chargerNear.split(',');
    url = new URL('https://api.tomtom.com/search/2/nearbySearch/.json');
    url.search = new URLSearchParams({lat, lon, radius:'25000', limit:'12', minPowerKW:'60.01'});
  } else if (typeof body?.query === 'string' && body.query.trim() && body.query.length <= 500) {
    url = new URL('https://api.tomtom.com/search/2/search/' + encodeURIComponent(body.query.trim()) + '.json');
    url.searchParams.set('countrySet', 'GB');
    url.searchParams.set('limit', body.suggestions ? '5' : '1');
    if (body.suggestions) url.searchParams.set('typeahead', 'true');
    url.searchParams.set('language', 'en-GB');
  } else if (Array.isArray(body?.coords) && body.coords.length >= 2 && body.coords.length <= 150
    && body.coords.every(coordinate)) {
    url = new URL('https://api.tomtom.com/routing/1/calculateRoute/' + body.coords.join(':') + '/json');
    url.searchParams.set('traffic', 'true');
    url.searchParams.set('routeType', 'fastest');
    url.searchParams.set('computeTravelTimeFor', 'all');
    if (body.chargerDrive) url.searchParams.set('routeRepresentation', 'summaryOnly');
    else url.searchParams.set('sectionType', 'traffic');
  } else {
    return Response.json({error:'Invalid search or route'},{status:400});
  }
  url.searchParams.set('key', key);

    return fetch(url,{signal,cache:'no-store',credentials:'omit'});
  }
  async function geocode(address){
    if(address===homeAddress)return homeCoordinate;
    const current=String(address).match(/^Current Location \[(-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?)\]$/);
    if(current && Math.abs(Number(current[1]))<=90 && Math.abs(Number(current[2]))<=180)return current[1]+','+current[2];
    if(selectedPlaces.has(address))return selectedPlaces.get(address);const r=await tomTom({query:address});if(!r.ok)throw Error(r.status===401?'TomTom rejected the key. Check its website and product restrictions.':`Place search failed (${r.status})`);const d=await r.json(),p=d.results?.[0]?.position;if(!p)throw Error(`Could not find “${address}”.`);return `${p.lat},${p.lon}`}
  function trafficReasons(route){
    const categories={JAM:'Congestion',ROAD_WORK:'Roadworks',ROAD_CLOSURE:'Road closure'};
    const incidents=(route.sections||[]).filter(section=>section.sectionType==='TRAFFIC');
    let offset=0;
    return route.legs.map(leg=>{
      const end=offset+(leg.points?.length||0)-1;
      const incident=incidents.filter(section=>section.startPointIndex<end && section.endPointIndex>offset && categories[section.simpleCategory])
        .sort((a,b)=>(b.delayInSeconds||0)-(a.delayInSeconds||0))[0];
      offset=end+1;
      return categories[incident?.simpleCategory];
    });
  }
  let routeRequest=0;
  async function calcRoute(){
    const request=++routeRequest;
    const names=routeNames();
    if(names.some(name=>!name || name==='Home')){
      $('calculate').disabled=false;
      status('Add a start address, destination and a place for every stop.');
      return;
    }
    const signature=JSON.stringify(names);
    const button=$('calculate');
    button.disabled=true;
    status('Getting current travel times…');
    try{
      const coords=[];
      for(const name of names)coords.push(await geocode(name));
      const response=await tomTom({coords});
      if(!response.ok)throw Error('TomTom routing failed ('+response.status+').');
      const data=await response.json();
      const route=data.routes?.[0];
      const legs=route?.legs;
      if(legs?.length!==names.length-1 || legs.some(leg=>!Number.isFinite(leg.summary?.travelTimeInSeconds)))throw Error('TomTom returned an incomplete route.');
      if(request!==routeRequest || signature!==JSON.stringify(routeNames()))return;
      const minutes=leg=>Math.max(0,Math.round(leg.summary.travelTimeInSeconds/60));
      state.stops.forEach((stop,index)=>{stop.drive=minutes(legs[index]);});
      state.destinationDrive=minutes(legs.at(-1));
      routeData={signature,coords,reasons:trafficReasons(route),paths:legs.map(leg=>leg.points),delays:legs.map(leg=>Math.ceil(Math.max(0,leg.summary.trafficDelayInSeconds||0)/60))};
      state.cachedRoute=routeData;
      if(gpsFix)trackDeparture(gpsFix);
      state.journeyTracking=departureTracking;
      render();
      status('');
      void loadWeather(coords);
    }catch(error){
      if(request===routeRequest)status((error.message||'Travel times unavailable.')+' Existing times kept.');
    }finally{
      if(request===routeRequest)button.disabled=false;
    }
  }
  function routeLookupSamples(points){
    const samples=[];
    let distance=0;
    let next=4;
    for(let i=1;i<points.length;i++){
      const a=points[i-1],b=points[i];
      const length=111*Math.hypot(b.latitude-a.latitude,
        (b.longitude-a.longitude)*Math.cos(a.latitude*Math.PI/180));
      while(next<distance+length){
        const fraction=(next-distance)/length;
        samples.push((a.latitude+(b.latitude-a.latitude)*fraction).toFixed(6)+','+
          (a.longitude+(b.longitude-a.longitude)*fraction).toFixed(6));
        next+=8;
      }
      distance+=length;
    }
    if(!samples.length && points.length>2){
      const middle=points[Math.floor(points.length/2)];
      samples.push(middle.latitude+','+middle.longitude);
    }
    return samples;
  }

  async function addFacilityStop(place,button,signal,facility){
    const names=routeNames();
    const signature=JSON.stringify(names);
    if(names.some(name=>!name || name==='Home')){
      button.textContent='Complete your route first';
      return;
    }
    for(const choice of $('ev-results').querySelectorAll('.ev-add'))choice.disabled=true;
    const label=button.textContent;
    button.textContent='Finding best position…';
    try{
      const coords=await Promise.all(names.map(name=>geocode(name)));
      const baseline=await tomTom({coords},signal);
      if(!baseline.ok)throw Error();
      const legs=(await baseline.json()).routes?.[0]?.legs;
      if(legs?.length!==coords.length-1)throw Error();
      const options=await Promise.all(legs.map(async (leg,index)=>{
        const original=leg.summary?.travelTimeInSeconds;
        const route=await tomTom({coords:[coords[index],place.coord,coords[index+1]],chargerDrive:true},signal);
        if(!route.ok)throw Error();
        const seconds=(await route.json()).routes?.[0]?.summary?.travelTimeInSeconds;
        if(!Number.isFinite(seconds)||!Number.isFinite(original))throw Error();
        return {index,extra:seconds-original};
      }));
      if(signal.aborted || signature!==JSON.stringify(routeNames()))return;
      const {index}=options.sort((a,b)=>a.extra-b.extra)[0];
      const name=place.address && !place.name.endsWith(place.address) ? place.name+', '+place.address : place.name;
      selectedPlaces.set(name,place.coord);
      state.places=Object.fromEntries(selectedPlaces);
      if(index<state.stops.length)state.stops[index].drive='';
      else state.destinationDrive='';
      state.stops.splice(index,0,{name,drive:'',stay:20,facility});
      evDialog.close();
      render();
      await calcRoute();
    }catch{
      if(!signal.aborted)button.textContent='Could not check detour · tap to retry';
    }finally{
      if(!signal.aborted){
        if(button.textContent==='Finding best position…')button.textContent=label;
        for(const choice of $('ev-results').querySelectorAll('.ev-add'))choice.disabled=false;
      }
    }
  }

  const evDialog=$('ev-dialog');
  let evLookup;
  evDialog.addEventListener('close',()=>evLookup?.abort());
  $('ev-lookup').onclick=()=>lookupFacilities(false);
  $('toilet-lookup').onclick=()=>lookupFacilities(true);
  async function lookupFacilities(toilets){
    const label=toilets?'public toilets':'chargers';
    $('ev-title').textContent=toilets?'Public toilets':'EV chargers';
    $('ev-note').textContent=(toilets?'Up':'Over 60 kW and up')+' to 10 minutes’ drive from a stop, your destination or a checked point on the route.';
    evLookup?.abort();
    evLookup=new AbortController();
    const {signal}=evLookup;
    const output=$('ev-results');
    const stops=state.stops.map((stop,index)=>({name:stop.name,label:'Stop '+(index+1),event:timelineEvents[index+1]}));
    stops.push({name:state.destination,label:'End',event:timelineEvents.at(-1)});
    const places=stops.filter(stop=>stop.name.trim());
    output.textContent=places.length?'Looking up '+label+'…':'Add a stop or destination first.';
    evDialog.showModal();
    if(!places.length)return;
    const routeStatus=document.createElement('p');
    routeStatus.className='ev-context';
    output.replaceChildren(routeStatus);
    const names=routeNames();
    const legs=[];
    if(names.every(name=>name && name!=='Home')){
      routeStatus.textContent='Finding the roads between your stops…';
      try{
        let paths=currentRoute()?.paths;
        if(!paths){
          const coords=await Promise.all(names.map(name=>geocode(name)));
          const response=await tomTom({coords},signal);
          if(!response.ok)throw Error();
          paths=(await response.json()).routes?.[0]?.legs?.map(leg=>leg.points);
        }
        if(!paths?.length)throw Error();
        const labels=[state.start,...state.stops.map(stop=>stop.name),state.destination];
        paths.forEach((points,index)=>{
          const anchors=routeLookupSamples(points||[]);
          if(anchors.length)legs.push({label:'On the road',name:labels[index].split(',')[0]+' → '+labels[index+1].split(',')[0],anchors});
        });
        routeStatus.textContent='Checking stops, destination and points along each driving leg.';
      }catch{
        if(signal.aborted)return;
        routeStatus.textContent='Could not load the driving route. Checking individual stops and destination.';
      }
    }else{
      routeStatus.textContent='Complete the route to also check the roads between stops.';
    }
    const found=new Map();
    const rows=new Map();
    const progress=document.createElement('p');
    progress.className='ev-context';
    progress.textContent='Checking driving times…';
    output.append(progress);
    let adding=false;

    function showMatch(key,place){
      let row=rows.get(key);
      if(!row){
        row=document.createElement('div');
        row.className='ev-station';
        rows.set(key,row);
        output.append(row);
      }
      if(adding)return;
      const button=document.createElement('button');
      button.type='button';
      button.className='ev-add';
      button.textContent=place.name+' ＋';
      button.setAttribute('aria-label','Add '+place.name+' as a stop');
      button.onclick=async ()=>{
        if(adding)return;
        adding=true;
        try{await addFacilityStop(found.get(key),button,signal,toilets?'toilet':'charger');}
        finally{
          adding=false;
          if(!signal.aborted)for(const [id,match] of found)if(id!==key)showMatch(id,match);
        }
      };
      const details=document.createElement('span');
      details.className='ev-details';
      details.textContent=(toilets?'':place.power+' kW · ')+Math.ceil(place.seconds/60)+' min drive from nearest checked point';
      row.replaceChildren(button);
      if(place.address && !place.name.endsWith(place.address)){
        const address=document.createElement('small');
        address.textContent=place.address;
        row.append(address);
      }
      const context=document.createElement('small');
      context.textContent=[...place.contexts].join(' / ');
      row.append(details,context);
    }
    let incomplete=false;
    for(const stop of [...places,...legs]){
      if(signal.aborted)return;
      const scheduled=stop.event && new Date(stop.event.forecastTime).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});
      const context=stop.label+' · '+stop.name.split(',')[0]+(scheduled?' · '+scheduled:'');
      progress.textContent='Checking '+context+'…';
      try{
        const anchors=stop.anchors||[await geocode(loc(stop.name))];
        for(const anchor of anchors){
          if(signal.aborted)return;
          try{
            const response=await tomTom(toilets?{toiletNear:anchor}:{chargerNear:anchor},signal);
            if(!response.ok)throw Error();
            const data=await response.json();
            const stations=(data.results||[]).map(place=>{
              const poi=place.poi?.name?.trim();
              const address=place.address?.freeformAddress||place.address?.municipality||'';
              const area=[place.address?.streetName,place.address?.municipality].filter(Boolean).join(', ');
              const generic=!poi || /^(public )?(toilets?|restrooms?|wc)$/i.test(poi);
              const name=toilets && generic
                ? 'Public toilets'+(area || address?' – '+(area||address):'')
                : poi||'EV charger';
              return {
                id:place.id,name,address,
                coord:place.position && place.position.lat+','+place.position.lon,
                power:Math.max(0,...(place.chargingPark?.connectors||[]).map(c=>Number(c.ratedPowerKW)||0))
              };
            }).filter(place=>place.coord && (toilets || place.power>60)).slice(0,12);
            const drives=await Promise.allSettled(stations.map(async place=>{
              if(place.coord===anchor)return {...place,seconds:0};
              const route=await tomTom({coords:[anchor,place.coord],chargerDrive:true},signal);
              if(!route.ok)throw Error();
              const body=await route.json();
              const seconds=body.routes?.[0]?.summary?.travelTimeInSeconds;
              if(!Number.isFinite(seconds))throw Error();
              return {...place,seconds};
            }));
            if(signal.aborted)return;
            incomplete ||= drives.some(result=>result.status==='rejected');
            for(const result of drives){
              if(result.status!=='fulfilled' || result.value.seconds>600)continue;
              const place=result.value;
              const key=place.id||place.coord;
              const previous=found.get(key);
              const contexts=previous?.contexts||new Set();
              contexts.add(context);
              if(!previous || place.seconds<previous.seconds)found.set(key,{...place,contexts});
              showMatch(key,found.get(key));
            }
          }catch{
            if(signal.aborted)return;
            incomplete=true;
          }
        }
      }catch{
        if(signal.aborted)return;
        incomplete=true;
      }
    }
    progress.textContent=found.size
      ? found.size+' matching locations'+(incomplete?' · some checks could not be completed':'')
      : incomplete?'Could not complete all '+label+' checks.':'No matching '+label+' found within 10 minutes.';
  }

  const selectedPlaces=new Map(Object.entries(state.places||{}));
  selectedPlaces.set(homeAddress,homeCoordinate);
  const results=document.createElement('div');
  results.id='place-results';
  results.className='place-results';
  results.setAttribute('role','listbox');
  results.setAttribute('aria-label','Place suggestions');
  results.hidden=true;

  let activeInput, lookupTimer, lookupRequest, highlighted=-1, choices=[];
  function closeLookup(){
    clearTimeout(lookupTimer);
    lookupRequest?.abort();
    activeInput?.setAttribute('aria-expanded','false');
    activeInput?.removeAttribute('aria-activedescendant');
    activeInput=null;
    results.hidden=true;
    choices=[];
    highlighted=-1;
  }

  function showResults(message){
    if(message)results.innerHTML='<div class="lookup-message" role="status">'+esc(message)+'</div>';
    activeInput.parentElement.append(results);
    results.hidden=false;
    activeInput.setAttribute('aria-expanded','true');
  }

  function pickPlace(index){
    const place=choices[index];
    if(!place || !activeInput)return;
    const input=activeInput;
    const address=place.address?.freeformAddress || '';
    const name=place.poi?.name;
    const label=name && address && !address.startsWith(name) ? name+', '+address : name||address;
    if(place.position)selectedPlaces.set(label,place.position.lat+','+place.position.lon);
    state.places=Object.fromEntries(selectedPlaces);
    closeLookup();
    input.value=label;
    input.dispatchEvent(new Event('input',{bubbles:true}));
    input.focus();
    void loadWeather([selectedPlaces.get(label)]);
  }

  document.addEventListener('input',event=>{
    const input=event.target;
    if(!input.matches('#start,#destination,[data-field="name"]'))return;
    closeLookup();
    const query=input.value.trim();
    const preferred=preferredStore(query);
    if((query.length<3 && !preferred) || /^home$/i.test(query) || selectedPlaces.has(query))return;
    activeInput=input;
    input.setAttribute('role','combobox');
    input.setAttribute('aria-autocomplete','list');
    input.setAttribute('aria-controls',results.id);
    input.setAttribute('aria-expanded','false');
    if(preferred){
      choices=[{poi:{name:preferred.name},address:{freeformAddress:preferred.address}}];
      results.innerHTML='<button type="button" role="option" tabindex="-1" aria-selected="true" id="place-option-0" data-place="0">'
        +esc(preferred.name)+'<small>'+esc(preferred.address)+'</small></button>';
      highlighted=0;
      input.setAttribute('aria-activedescendant','place-option-0');
      showResults();
      return;
    }
    lookupTimer=setTimeout(async()=>{
      const request=new AbortController();
      lookupRequest=request;
      showResults('Looking up places…');
      try{
        const response=await tomTom({query, suggestions:true},request.signal);
        if(!response.ok)throw Error('Lookup failed');
        const data=await response.json();
        if(request.signal.aborted || activeInput!==input)return;
        choices=(data.results||[]).filter(place=>Number.isFinite(place.position?.lat)
          && Number.isFinite(place.position?.lon) && (place.poi?.name || place.address?.freeformAddress));
        if(!choices.length){showResults('No matches. Try a town or postcode.');return;}
        results.innerHTML=choices.map((place,index)=>{
          const address=place.address?.freeformAddress||'';
          const name=place.poi?.name||address;
          return '<button type="button" role="option" tabindex="-1" aria-selected="false" id="place-option-'+index+'" data-place="'+index+'">'
            +esc(name)+(address&&address!==name?'<small>'+esc(address)+'</small>':'')+'</button>';
        }).join('');
        showResults();
      }catch{
        if(!request.signal.aborted && activeInput===input)showResults('Place lookup unavailable. Check your connection and TomTom Search access.');
      }
    },350);
  });
  results.addEventListener('pointerdown',event=>{
    if(event.target.closest('[data-place]'))event.preventDefault();
  });
  results.addEventListener('click',event=>{
    const option=event.target.closest('[data-place]');
    if(option)pickPlace(Number(option.dataset.place));
  });
  function placeValue(input){
    if(input.id==='start' || input.id==='destination')return state[input.id];
    const row=input.closest('[data-i]');
    return row && input.dataset.field==='name' ? state.stops[Number(row.dataset.i)]?.name : undefined;
  }
  document.addEventListener('focusin',event=>{
    const value=placeValue(event.target);
    if(value!==undefined)event.target.value=value;
  });
  document.addEventListener('focusout',event=>{
    if(event.target===activeInput)closeLookup();
    const value=placeValue(event.target);
    if(value!==undefined)event.target.value=shortPlace(value);
  });

  let gpsRequestPending=false, gpsSession=0;
  const gpsOptions={enableHighAccuracy:true,maximumAge:0,timeout:12000};
  function receiveLocation(position){
    if(gpsFix && position.timestamp<gpsFix.timestamp)return;
    state.liveLocation=true;
    gpsFix=position;
    trackDeparture(position);
    state.journeyTracking=departureTracking;
    persist();
    tick();
  }
  function locationError(error){
    if(error.code===1){
      stopLocation();
      $('gps-status').textContent='Location access denied. Allow location for Wayfinder in your phone settings, then retry GPS.';
    }else $('gps-status').textContent='Waiting for a fresh GPS reading. Keep Wayfinder visible.';
  }
  function stopLocation(){
    gpsSession++;
    if(gpsWatch!==undefined)navigator.geolocation.clearWatch(gpsWatch);
    gpsWatch=undefined;
    gpsFix=undefined;
    state.liveLocation=false;
    persist();
    $('gps-status').textContent='';
    drawProgress();
  }
  function requestFreshLocation(){
    if(document.hidden || gpsRequestPending || gpsWatch===undefined)return;
    gpsRequestPending=true;
    const session=gpsSession;
    navigator.geolocation.getCurrentPosition(position=>{
      gpsRequestPending=false;
      if(session===gpsSession)receiveLocation(position);
    },error=>{
      gpsRequestPending=false;
      if(session===gpsSession)locationError(error);
    },gpsOptions);
  }
  function startLocation(){
    if(gpsWatch!==undefined)return;
    if(!navigator.geolocation || !window.isSecureContext){
      $('gps-status').textContent='Live location needs an HTTPS site and browser location access.';
      return;
    }
    state.liveLocation=true;
    persist();
    $('gps-status').textContent='Waiting for GPS…';
    const session=++gpsSession;
    gpsWatch=navigator.geolocation.watchPosition(
      position=>{if(session===gpsSession)receiveLocation(position);},
      error=>{if(session===gpsSession)locationError(error);},
      gpsOptions);
  }
  setInterval(requestFreshLocation,15000);
  document.addEventListener('keydown',event=>{
    if(event.target!==activeInput || results.hidden)return;
    if(event.key==='Escape'){event.preventDefault();closeLookup();return;}
    if(!choices.length)return;
    if(event.key==='Enter' && highlighted>=0){
      event.preventDefault();
      pickPlace(highlighted);
    }
    if(event.key==='ArrowDown' || event.key==='ArrowUp'){
      event.preventDefault();
      highlighted=highlighted<0 ? (event.key==='ArrowDown'?0:choices.length-1)
        : (highlighted+(event.key==='ArrowDown'?1:-1)+choices.length)%choices.length;
      const options=[...results.querySelectorAll('[role="option"]')];
      options.forEach((option,index)=>option.setAttribute('aria-selected',String(index===highlighted)));
      activeInput.setAttribute('aria-activedescendant',options[highlighted].id);
      options[highlighted].scrollIntoView({block:'nearest'});
    }
  });
  function retryGps(){
    if(gpsWatch!==undefined)navigator.geolocation.clearWatch(gpsWatch);
    gpsWatch=undefined;
    startLocation();
    requestFreshLocation();
  };
  document.addEventListener('visibilitychange',()=>{
    if(!document.hidden && state.liveLocation)retryGps();
  });
  $('use-current-location').onclick=()=>{
    const button=$('use-current-location');
    if(!navigator.geolocation || !window.isSecureContext){
      status('Current Location needs browser location access on HTTPS.');
      return;
    }
    button.disabled=true;
    status('Finding your current location…');
    navigator.geolocation.getCurrentPosition(position=>{
      button.disabled=false;
      if(position.coords.accuracy>50){
        status('Location is accurate to '+Math.round(position.coords.accuracy)+' m. Move somewhere with a clearer GPS signal and try Current Location again.');
        return;
      }
      const latitude=position.coords.latitude.toFixed(6);
      const longitude=position.coords.longitude.toFixed(6);
      const label='Current Location ['+latitude+' '+longitude+']';
      routeRequest++;
      routeData=undefined;
      delete state.cachedRoute;
      state.start=label;
      selectedPlaces.set(label,latitude+','+longitude);
      state.places=Object.fromEntries(selectedPlaces);
      gpsFix=position;
      render();
      startLocation();
      status('Start set to your current location.');
      if(routeNames().every(name=>name && name!=='Home'))void calcRoute();
    },error=>{
      button.disabled=false;
      status(error.code===1?'Allow location access in your phone settings, then tap Current Location again.':'Could not get your current location. Please try again.');
    },{enableHighAccuracy:true,maximumAge:0,timeout:20000});
  };
  $('calculate').onclick=()=>{
    startLocation();
    void calcRoute();
  };
  setInterval(tick,1000);
  render();
  if(state.liveLocation){
    startLocation();
    if(!routeNames().some(name=>!name || name==='Home'))void calcRoute();
  }
  void loadWeather(routeNames().map(name=>selectedPlaces.get(name)));
  if('serviceWorker' in navigator)window.addEventListener('load',()=>navigator.serviceWorker.register('./sw.js').catch(()=>{}));
})();
