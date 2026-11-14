import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildSync } from 'esbuild';
const code=buildSync({entryPoints:['src/policy.ts'],bundle:true,platform:'node',format:'esm',write:false}).outputFiles[0].text;
const {needsJobs,repairReason}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
test('live job accounting is independent of historical detail collection limits',()=>{
 const clock=1000000,run={workflow:'.github/workflows/labels-from-comments.yml',status:'queued',created:clock-3*86400,updated:clock-3*86400};
 assert.equal(repairReason('jobs',run,clock),null);
 assert.equal(needsJobs({run,jobs:0,job_rows:[],verified:false}),true);
 assert.equal(needsJobs({run,jobs:0,job_rows:[],verified:true}),false);
 assert.equal(needsJobs({run,jobs:1,job_rows:[{status:'pending',estimated:false}],verified:false}),false);
 assert.equal(repairReason('details',run,clock),'auxiliary jobs use webhook observations');
 assert.equal(repairReason('jobs',{...run,status:'completed'},clock),'auxiliary jobs use webhook observations');
});
