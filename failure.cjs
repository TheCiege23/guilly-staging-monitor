async function main(){
const env=JSON.parse(process.env.GUILLY_STAGING_ENV);
const run='https://github.com/'+process.env.GITHUB_REPOSITORY+'/actions/runs/'+process.env.GITHUB_RUN_ID;
const response=await fetch('https://api.resend.com/emails',{method:'POST',signal:AbortSignal.timeout(30000),headers:{Authorization:'Bearer '+env.RESEND_API_KEY,'Content-Type':'application/json','Idempotency-Key':'guilly-workflow-'+process.env.GITHUB_RUN_ID},body:JSON.stringify({from:env.RESEND_FROM,to:[env.GUILLY_ALERT_TO],subject:'Guilly staging: hosted workflow failure',text:'The isolated staging monitor workflow failed. Review '+run+'. Production was not targeted.'})});
if(!response.ok) throw Error('Failure email HTTP '+response.status);
console.log('Hosted workflow failure email accepted');
}main().catch(e=>{console.error(e.message);process.exitCode=1});
