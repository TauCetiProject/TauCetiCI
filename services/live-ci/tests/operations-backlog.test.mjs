import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { build } from 'esbuild';
const bundled = await build({entryPoints:['src/operations-backlog.ts'],bundle:true,platform:'node',format:'esm',write:false});
const {backlogSnapshot, mergeBacklogs} = await import('data:text/javascript;base64,'+Buffer.from(bundled.outputFiles[0].text).toString('base64'));
const repo='TauCetiProject/TauCeti', clock=100000;
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
  const result=backlogSnapshot(f.sql,clock);
  assert.deepEqual(result.pr_builds,{pending:1,queued:1,in_progress:1,stuck:2,unconfirmed:2,oldest_waiting_at:clock-10000});
  assert.equal(result.other_workflows.stuck,1);
  assert.equal(result.waiting_jobs,2);assert.equal(result.running_jobs,1);
});
test('synthetic placeholders are not real queued jobs; old attempts do not count twice',t=>{
  const f=fixture(t);f.run(1);f.job(1,1,'pending',{estimated:true});f.verified(1);
  f.run(2,'pending');f.job(2,2);f.run(2,'completed',{attempt:2});
  const result=backlogSnapshot(f.sql,clock);
  assert.equal(result.pr_builds.unconfirmed,1);assert.equal(result.pr_builds.stuck,0);assert.equal(result.pr_builds.pending,0);assert.equal(result.waiting_jobs,0);
});
test('counts precede run, job and partition display limits; missing sources stay partial',t=>{
  const f=fixture(t);for(let i=1;i<=1201;i++)f.run(i,'pending');
  const backlog=backlogSnapshot(f.sql,clock);assert.equal(backlog.pr_builds.pending,1201);
  const sources=Array.from({length:205},(_,i)=>({partition:repo+'/'+i,backlog}));
  let result=mergeBacklogs(sources,false);assert.equal(result.repositories[repo].pr_builds.pending,1201*205);assert.equal(result.coverage,'observed');
  result=mergeBacklogs([...sources,{partition:repo+'/missing'}],false);assert.equal(result.coverage,'partial');
  assert.equal(mergeBacklogs(sources,true).coverage,'partial');
});
