import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { build } from 'esbuild';
const bundled = await build({entryPoints:['src/operations-backlog.ts'],bundle:true,platform:'node',format:'esm',write:false});
const {backlogSnapshot, mergeBacklogs, queuedHours} = await import('data:text/javascript;base64,'+Buffer.from(bundled.outputFiles[0].text).toString('base64'));
const repo='TauCetiProject/TauCeti', clock=1000000;
// Published counts: a snapshot classifies zombies only when merged by a clock.
const summary=(f,at=clock)=>mergeBacklogs([{partition:repo+'/p',backlog:backlogSnapshot(f.sql,at)}],false,at).repositories[repo];
function fixture(t) {
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  db.exec('CREATE TABLE runs(id TEXT,attempt INTEGER,data TEXT); CREATE TABLE jobs(id TEXT,run TEXT,attempt INTEGER,active INTEGER,data TEXT); CREATE TABLE meta(key TEXT,value TEXT)');
  const sql={exec(q,...args){return{toArray:()=>db.prepare(q).all(...args)}}};
  const run=(id,status='queued',options={})=>db.prepare('INSERT INTO runs VALUES(?,?,?)').run(String(id),options.attempt||1,JSON.stringify({repo,id:String(id),attempt:1,workflow:'.github/workflows/pr-build.yml',status,created:clock-10000,updated:clock-10000,started:clock-10000,...options}));
  const job=(id,run,status='pending',options={})=>db.prepare('INSERT INTO jobs VALUES(?,?,?,?,?)').run(String(id),String(run),options.attempt||1,status==='completed'?0:1,JSON.stringify({status,assigned:status==='in_progress',estimated:false,...options}));
  const verified=(id)=>db.prepare('INSERT INTO meta VALUES(?,?)').run('verified/'+id+'/1','true');
  return {sql,run,job,verified};
}
test('pending jobs survive any age; only verified terminal attempts become stuck',t=>{
  const f=fixture(t);
  f.run(1,'pending');f.job(1,1);f.verified(1);
  f.run(2);f.verified(2);
  f.run(3);f.job(3,3,'completed');f.verified(3);
  f.run(4); // no enumeration: never call this confirmed stuck
  f.run(5,'queued',{updated:clock-60});f.verified(5);
  f.run(6);f.job(6,6,'queued');
  f.run(7,'in_progress');f.job(7,7,'in_progress');
  f.run(8,'queued',{workflow:'.github/workflows/pr-status.yml'});f.verified(8);
  const result=summary(f);
  assert.deepEqual(result.pr_builds,{pending:1,queued:3,in_progress:1,stuck:2,unconfirmed:0,stale:0,oldest_waiting_at:clock-10000});
  assert.equal(result.other_workflows.stuck,1);
  assert.equal(result.waiting_jobs,2);assert.equal(result.running_jobs,1);
});
test('synthetic placeholders are not real queued jobs; old attempts do not count twice',t=>{
  const f=fixture(t);f.run(1);f.job(1,1,'pending',{estimated:true});f.verified(1);
  f.run(2,'pending');f.job(2,2);f.run(2,'completed',{attempt:2});
  const result=summary(f);
  assert.equal(result.pr_builds.unconfirmed,0);assert.equal(result.pr_builds.queued,1);assert.equal(result.pr_builds.stuck,0);assert.equal(result.pr_builds.pending,0);assert.equal(result.waiting_jobs,0);
});
test('observed waiting or running jobs take precedence over stale parent workflow state',t=>{
  const f=fixture(t);
  f.run(1,'in_progress');f.job(1,1,'pending');
  f.run(2,'pending');f.job(2,2,'in_progress');
  const result=summary(f);
  assert.equal(result.pr_builds.pending,1);assert.equal(result.pr_builds.in_progress,1);
  assert.equal(result.pr_builds.oldest_waiting_at,clock-10000);
});
test('a pending workflow keeps its place even after a previously empty enumeration',t=>{
  const f=fixture(t);f.run(1,'pending');f.verified(1);
  const result=summary(f);
  assert.equal(result.pr_builds.pending,1);assert.equal(result.pr_builds.stuck,0);
});
test('counts precede run, job and partition display limits; missing sources stay partial',t=>{
  const f=fixture(t);for(let i=1;i<=1201;i++)f.run(i,'pending');
  const backlog=backlogSnapshot(f.sql,clock);assert.equal(backlog.pr_builds.pending,1201);
  const sources=Array.from({length:205},(_,i)=>({partition:repo+'/'+i,backlog}));
  let result=mergeBacklogs(sources,false,clock);assert.equal(result.repositories[repo].pr_builds.pending,1201*205);assert.equal(result.coverage,'observed');
  result=mergeBacklogs([...sources,{partition:repo+'/missing'}],false,clock);assert.equal(result.coverage,'partial');
  assert.equal(mergeBacklogs(sources,true,clock).coverage,'partial');
});

test('normal GitHub workflow scheduling states are accounted even before jobs exist',t=>{
 const f=fixture(t);
 for (const [id,status] of [[1,'queued'],[2,'pending'],[3,'waiting'],[4,'requested'],[5,'in_progress']]) f.run(id,status);
 const result=summary(f);
 assert.equal(result.pr_builds.queued,1);assert.equal(result.pr_builds.pending,4);
 assert.equal(result.pr_builds.unconfirmed,0);assert.equal(result.waiting_jobs,0);
 assert.equal(backlogSnapshot(f.sql,clock).accounting_version,3);
 f.run(6,'unknown-status');assert.equal(summary(f).pr_builds.unconfirmed,1);
});

test('known GitHub zombies leave waiting counts and the oldest wait once a day old, by the merge clock',t=>{
  const f=fixture(t), day=86400, old=clock-2*day;
  f.run(1,'queued',{created:old,updated:old,started:old}); // no jobs at all
  f.run(2,'queued',{created:old,updated:old,started:old});f.verified(2); // verified empty: was stuck
  f.run(3,'in_progress',{created:old});f.job(3,3,'queued',{created:old}); // completion missed
  f.run(4,'queued',{workflow:'.github/workflows/pr-status.yml',created:old,updated:old,started:old});
  f.run(5,'pending',{created:old});f.job(5,5,'pending',{created:clock-60}); // live: keeps its place
  f.run(6,'queued');f.job(6,6,'queued',{created:clock-60}); // live
  let result=summary(f);
  assert.deepEqual(result.pr_builds,{pending:1,queued:1,in_progress:0,stuck:0,unconfirmed:0,stale:3,oldest_waiting_at:old});
  assert.equal(result.other_workflows.stale,1);assert.equal(result.other_workflows.queued,0);
  assert.equal(result.waiting_jobs,2);assert.equal(result.stale_waiting_jobs,1);
  assert.equal(result.aging,undefined);assert.equal(result.waiting_created,undefined);assert.equal(result.pr_builds.waiting_runs,undefined);
  // Without run 5, the oldest wait is the live job's workflow, not the zombie's.
  const g=fixture(t);g.run(3,'in_progress',{created:old});g.job(3,3,'queued',{created:old});
  g.run(6,'queued',{created:clock-120});g.job(6,6,'queued',{created:clock-60});
  assert.equal(summary(g).pr_builds.oldest_waiting_at,clock-120);
  // A cached snapshot still classifies by the merge's clock as it ages.
  const h=fixture(t), young=clock-3600*2;
  h.run(1,'queued',{created:young,updated:young,started:young});h.run(2,'queued',{created:young});h.job(2,2,'queued',{created:young});
  const snapshot=backlogSnapshot(h.sql,clock), merge=at=>mergeBacklogs([{partition:repo+'/p',backlog:snapshot}],false,at).repositories[repo];
  assert.deepEqual([merge(clock).pr_builds.queued,merge(clock).pr_builds.stale,merge(clock).waiting_jobs,merge(clock).pr_builds.oldest_waiting_at],[2,0,1,young]);
  const later=merge(clock+day);
  assert.deepEqual([later.pr_builds.queued,later.pr_builds.stale,later.waiting_jobs,later.stale_waiting_jobs,later.pr_builds.oldest_waiting_at],[0,2,0,1,null]);
});

test('a fresh re-run of an old workflow is live; older snapshots stay out of published counts',t=>{
  const f=fixture(t), old=clock-2*86400;
  f.run(1,'queued',{attempt:2,created:old,updated:clock-60,started:clock-60});
  let result=summary(f);assert.equal(result.pr_builds.queued,1);assert.equal(result.pr_builds.stale,0);
  const merged=mergeBacklogs([{partition:repo+'/p',backlog:{...backlogSnapshot(f.sql,clock),accounting_version:2}}],false,clock);
  assert.equal(merged.coverage,'partial');assert.equal(merged.repositories[repo],undefined);
  assert.deepEqual(queuedHours([{status:'queued',created:old,started:old},{status:'pending',created:old},{status:'queued',created:old,started:clock-60}]),
    [[Math.floor(old/3600)*3600,1],[Math.floor((clock-60)/3600)*3600,1]]);
});

test('an empty snapshot from older accounting counts as observed; a nonempty one stays out',t=>{
  const zero={pending:0,queued:0,in_progress:0,stuck:0,unconfirmed:0,oldest_waiting_at:null};
  const empty={accounting_version:2,pr_builds:zero,other_workflows:zero,waiting_jobs:0,running_jobs:0};
  let result=mergeBacklogs([{partition:repo+'/a',backlog:empty}],false,clock);
  assert.equal(result.coverage,'observed');assert.equal(result.repositories[repo].waiting_jobs,0);
  result=mergeBacklogs([{partition:repo+'/a',backlog:empty},{partition:repo+'/b',backlog:{...empty,other_workflows:{...zero,queued:3}}}],false,clock);
  assert.equal(result.coverage,'partial');assert.equal(result.repositories[repo].other_workflows.queued,0);
  assert.equal(mergeBacklogs([{partition:repo+'/c',backlog:{accounting_version:2}}],false,clock).coverage,'partial');
});
