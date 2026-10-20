const {spawnSync}=require('node:child_process');
const selected=JSON.parse(process.env.GUILLY_STAGING_ENV);
for(const value of Object.values(selected)) if(value) console.log('::add-mask::'+String(value).replace(/%/g,'%25').replace(/\r/g,'%0D').replace(/\n/g,'%0A'));
const env={...process.env,...selected,GUILLY_MONITOR_ONCE:'1',GUILLY_DASHBOARD_URL:'https://github.com/TheCiege23/guilly-staging-monitor/actions'};
delete env.GUILLY_STAGING_ENV;
const result=spawnSync(process.execPath,['monitor.cjs'],{env,stdio:'inherit',timeout:1200000});
process.exitCode=result.status??1;
