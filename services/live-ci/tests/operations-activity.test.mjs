import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {buildSync} from 'esbuild';
const code=buildSync({entryPoints:['src/operations-activity.ts'],bundle:true,platform:'node',format:'esm',write:false}).outputFiles[0].text;
const {ActivityIndex,mergeActivity}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const clock=Date.parse('2026-11-06T20:00:30Z')/1000, through=Math.floor(clock/60)*60;
const run={repo:'TauCetiProject/TauCeti',id:'100',attempt:1,workflow:'.github/workflows/pr-build.yml',event:'pull_request_target',trigger:'pr',status:'in_progress',conclusion:null,created:clock-10000,started:clock-10000,updated:clock,head:'a'.repeat(40),branch:'main',url:''};
const job=(id,changes={})=>({id:String(id),run:'100',attempt:1,name:'sandboxed-build',status:'completed',conclusion:'success',created:clock-1000,started:clock-500,completed:through-120,kind:'github',assigned:true,steps:[],weight:1,estimated:false,updated:clock,...changes});
function runtime(t){
 const db=new DatabaseSync(':memory:');t.after(()=>db.close());
 const sql={exec(query,...args){let rows=[];if(!args.length&&query.split(';').filter(s=>s.trim()).length>1)db.exec(query);else{const stmt=db.prepare(query);if(stmt.columns().length)rows=stmt.all(...args);else stmt.run(...args);}return{toArray:()=>rows,one:()=>rows[0]};}};
 sql.exec('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE jobs(id TEXT PRIMARY KEY,run TEXT,attempt INTEGER,active INTEGER,data TEXT);');
 const index=new ActivityIndex(sql);
 return{sql,index,ready(){sql.exec("INSERT OR REPLACE INTO meta VALUES ('activity_index_complete','true')");},snapshot(at=clock){return index.snapshot(at);}};
}
const pool=(snapshot,name='github')=>snapshot.pools.find(p=>p.pool===name);
const merged=(r,at=clock)=>mergeActivity([{activity:r.snapshot()}],at,false);
test('counts precede public display limits; duplicate observations and corrections are reversible',t=>{
 const r=runtime(t);r.ready();
 for(let i=0;i<1601;i++)r.index.record(run,job(10000+i),0);
 r.index.record(run,job(10000),0);r.index.record(run,job(10001,{conclusion:'failure'}),0);
 let value=pool(merged(r));
 assert.equal(value.builds.windows[0].finished,1601);assert.equal(value.builds.windows[0].success,1600);assert.equal(value.builds.windows[0].failure,1);
 r.index.record(run,job(10001,{name:'control',conclusion:'cancelled'}),0);value=pool(merged(r));
 assert.equal(value.builds.windows[0].finished,1600);assert.equal(value.all.windows[0].cancelled,1);
 r.index.remove('10000');assert.equal(pool(merged(r)).builds.windows[0].finished,1599);
});
test('minute-aligned windows expire from cached facts without refetching quiet sources',t=>{
 const r=runtime(t);r.ready();[-86401,-86400,-10800,-3600,-1,0,20].forEach((n,i)=>r.index.record(run,job(100+i,{completed:through+n}),0));
 const cached=[{activity:r.snapshot()}], first=pool(mergeActivity(cached,clock,false)), next=pool(mergeActivity(cached,clock+60,false));
 assert.deepEqual(first.all.windows.map(w=>w.finished),[2,3,4]);assert.deepEqual(next.all.windows.map(w=>w.finished),[3,4,5]);
 assert.equal(first.all.latest_completion.id,'106');assert.equal(mergeActivity(cached,clock,false).through,new Date(through*1000).toISOString());
});
test('outcomes and build scope are explicit; estimated and inferred parent completions do not count',t=>{
 const r=runtime(t);r.ready();['success','failure','timed_out','cancelled','skipped',null].forEach((conclusion,i)=>r.index.record(run,job(100+i,{conclusion}),0));
 r.index.record(run,job(200,{name:'pick-runner'}),0);r.index.record(run,job(201,{estimated:true}),0);
 r.index.record({...run,status:'completed'},job(202,{status:'in_progress',completed:null}),0);
 const p=pool(merged(r));assert.deepEqual(p.builds.windows[0],{seconds:3600,finished:6,success:1,failure:2,cancelled:1,skipped:1,other:1});
 assert.equal(p.all.windows[0].finished,7);assert.equal(p.all.running,0);
});
test('oldest running uses actual starts, waiting uses creation, and references link exact jobs',t=>{
 const r=runtime(t);r.ready();
 r.index.record(run,job(100,{status:'queued',assigned:false,created:clock-9000,started:clock-9000,completed:null}),1);
 r.index.record(run,job(101,{status:'in_progress',created:clock-5000,started:clock-20,completed:null}),1);
 r.index.record(run,job(102,{status:'in_progress',created:clock-4000,started:clock-300,completed:null}),1);
 const p=pool(merged(r));assert.equal(p.builds.running,2);assert.equal(p.builds.waiting,1);assert.equal(p.builds.oldest_running.id,'102');assert.equal(p.builds.oldest_waiting.id,'100');
 assert.equal(p.builds.oldest_running.url,'https://github.com/TauCetiProject/TauCeti/actions/runs/100/job/102');
});
test('backfill is bounded, restartable and partial until every retained fact is indexed',t=>{
 const r=runtime(t);for(let i=0;i<1201;i++){const j=job(10000+i);r.sql.exec('INSERT INTO jobs VALUES (?,?,?,?,?)',j.id,j.run,1,0,JSON.stringify(j));}
 r.index.backfill(()=>run);assert.equal(r.sql.exec('SELECT count(*) n FROM ops_activity').one().n,500);assert.equal(merged(r).coverage,'partial');
 new ActivityIndex(r.sql).backfill(()=>run);assert.equal(r.sql.exec('SELECT count(*) n FROM ops_activity').one().n,1000);
 r.index.backfill(()=>run);assert.equal(merged(r).coverage,'retained_observations');assert.equal(pool(merged(r)).builds.windows[0].finished,1201);assert.equal(r.index.backfill(()=>run),false);
 r.index.removeRun('100',1);assert.equal(pool(merged(r)).builds.windows[0].finished,0);
});
test('partition merge preserves pool aliases, latest outcome and explicit missing coverage',t=>{
 const a=runtime(t),b=runtime(t);a.ready();b.ready();a.index.record(run,job(101,{kind:'yael',completed:through-120}),0);b.index.record(run,job(102,{kind:'yael',completed:through-60,conclusion:'cancelled'}),0);
 const facts=[{activity:a.snapshot()},{activity:b.snapshot()}];assert.equal(pool(mergeActivity(facts,clock,false),'yael').all.windows[0].finished,2);assert.equal(pool(mergeActivity(facts,clock,false),'yael').all.latest_completion.id,'102');
 assert.equal(mergeActivity(facts,clock,true).coverage,'partial');assert.equal(mergeActivity([{jobs:[]}],clock,false).coverage,'partial');assert.equal(mergeActivity([],clock,false).coverage,'unavailable');
});
test('an unknown running start remains unknown across partitions instead of claiming a known oldest age',t=>{
 const a=runtime(t),b=runtime(t);a.ready();b.ready();
 a.index.record(run,job(100,{status:'in_progress',started:clock-300,completed:null}),1);
 b.index.record(run,job(101,{status:'in_progress',started:null,completed:null}),1);
 const p=pool(mergeActivity([{activity:a.snapshot()},{activity:b.snapshot()}],clock,false));
 assert.equal(p.all.oldest_running.id,'101');assert.equal(p.all.oldest_running.started_at,null);
});
