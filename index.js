const CronParser = require('cron-parser');
const Vm = require('@ijstech/vm');
const Module = require('@ijstech/module');
const Log = require('@ijstech/log');
const Url = require('url');
const https = require('https');
const http = require('http');

var Jobs = [];
var Options; 
var ProcessJobTimer;
var ModuleIdx = {};
var PackageIdx = {};
var UpdateServerConfig; 
function getScript(module){    
    let result = '';    
    if (module.reference){
        for (let i = 0; i < module.reference.length; i ++)
            result += getScript(module.reference[i]);
    }
    result += module.script || ''
    return result;
}
function Post(url, data, headers){    
    return new Promise(function(resolve, reject){        
        url = Url.parse(url);
        headers = headers || {};
        if (typeof(data) != 'undefined'){
            if (typeof(data) != 'string'){
                data = JSON.stringify(data);
                if (!headers['Content-Type'])
                    headers['Content-Type'] = 'application/json';
            }
            headers['Content-Length'] = data.length;
        }                
        const options = {
            hostname: url.hostname,
            path: url.path,
            method: 'POST',
            headers: headers
        };
        
        function callback(res){            
            let data = '';
            let contentType = res.headers['content-type'];            
            res.on('data', (chunk) => {
                data += chunk;
            });
            res.on('end', ()=>{
                if (contentType && contentType.indexOf('json'))                
                    resolve(JSON.parse(data))
                else
                    resolve(data)
            })
        }
        let req;        
        if (url.protocol == 'https')
            req = https.request(options, callback)
        else
            req = http.request(options, callback);
        
        req.on('error', (err)=>{               
            reject(err);
        })
        req.write(data || '');
        req.end();
    })
}
async function getModuleCode(config, moduleId){
    try{        
        let result = await Post(config.host, {
            path: moduleId,
            token: config.token,
            code: true
        });        
        if (typeof(result) == 'string')
            return JSON.parse(result).code
        else if (typeof(result) == 'object')
            return result.code
    }
    catch(err){        
        Log.error(err);
        return;
    }
}
async function getModuleScript(config, moduleId){    
    try{
        var result = await Post(config.host, {
            path: moduleId,
            token: config.token,        
            script: true,
            code: true
        });        
        if (typeof(result) == 'string')
            result = JSON.parse(result)
        
        return {
            require: result.requiredModules,
            script: getScript(result)
        }
    }
    catch(err){        
        Log.error(err);
        return;
    }
}

function runJob(job, script){    
    return new Promise(async function(resolve){
        let vm = new Vm({
            org: job.org,
            logging: true,
            script: script.script,
            plugins: script.module.require || [],
            database: job.db,
            dbConfig: Options.db
        });
        try{            
            if (job.params)
                vm.injectGlobalObject('_params', JSON.parse(JSON.stringify(job.params)));
            else
                vm.injectGlobalObject('_params', {});
            await vm.eval(`
                (async function _untrusted() {
                    var result = await handleRequest(null, null, null, _params)
                })`)
        }
        catch(err){            
            Log.error(err);
        }
        finally{
            vm.destroy();
        }
        resolve();
    })
}
function getModule(job){
    let modulePath = job.module;    
    if (modulePath[0] != '/')
        modulePath = '/' + modulePath;    
    let module = ModuleIdx[(job.package.id + modulePath).toLowerCase()]     
    return module;
}
async function getJobScript(job){
    let module;
    if (job.package){
        module = getModule(job);
        if (!module)    
            return;
    }
    else
        module = {
            file: job.module
        }
    
    module.require = module.require || [];   
    let result = await Module.getModuleScript(job.package, module);      
    if (!result)
        return;
    if (module.require.indexOf('@ijstech/pdm') < 0 && Array.isArray(result.require)){
        for (let i = 0; i < result.require.length; i ++){
            let item = result.require[i].toLowerCase();
            if (item.substr(item.length - 4) == '.pdm'){
                module.require.push('@ijstech/pdm');
                break;
            }
        }
    }
    return {
        module: module,
        script: result.script
    };
}
function processJobs(){        
    return new Promise(async function(resolve){                
        for (let i = 0; i < Jobs.length; i ++){            
            let job = Jobs[i];
            let now = new Date();                        
            if (job.active && now.getTime() >= job.next.getTime()){
                let d1 = new Date();
                console.dir(`${new Date().toLocaleString()} ${job.module}`)
                try{                                 
                    let script = await getJobScript(job);
                    if (!script)
                        return resolve();
                    await runJob(job, script);
                    job.next = CronParser.parseExpression(job.cron).next();
                }
                catch(err){                    
                    Log.error(err);
                }
                finally{
                    let d2 = new Date();
                    console.dir(`Processing time: ${(Math.round(d2.getTime() - d1.getTime()) / 1000)} seconds`)
                }
            }                   
        }
        resolve();
    })
}
async function start(){    
    try{
        await processJobs();
    }
    catch(err){        
        Log.error(err);
    }    
    clearTimeout(ProcessJobTimer);
    ProcessJobTimer = null;
    ProcessJobTimer = setTimeout(start, 500);
}
module.exports = {
    _init: async function(options){             
        if (options.jobs){          
            Options = options;                  
            Jobs = [];
            let package = {}
            PackageIdx = {};
            ModuleIdx = {};
            for (let p in options.package){
                try{
                    let pack = options.package[p];                    
                    let packData = await Module.getPackage(p, pack)                        
                    for (let m in packData.modules){
                        let module = packData.modules[m];                        
                        let path = pack.id;
                        if (m[0] == '/')          
                            path = path + m.toLowerCase()
                        else
                            path = path + '/' + m.toLowerCase()
                        if (typeof(module) == 'string'){
                            module = {
                                id: module
                            }
                        }
                        ModuleIdx[path] = module;
                    }                    
                }
                catch(err){
                    Log.error(err);
                }            
            }
            for (let i = 0; i < options.jobs.length; i ++){            
                let job = options.jobs[i];
                Jobs.push({
                    org: typeof(job.org)=='string'?options.org[job.org]:job.org,
                    cron: job.cron,
                    db: job.db,
                    active: job.active==false?false:true,
                    params: job.params,
                    package: job.package?{
                        name: typeof(job.package)=='object'?job.package.name:job.package,
                        id: typeof(job.package)=='object'?job.package.id:options.package&&options.package[job.package]?options.package[job.package].id:job.package,
                        liveUpdate: typeof(job.package)=='object'?job.package.liveUpdate:options.package&&options.package[job.package]?options.package[job.package].liveUpdate:false
                    }:null,
                    module: job.module,
                    next: CronParser.parseExpression(job.cron).next()
                })       
            }
            start();
            console.dir('Scheduler enabled')
        }    
    }
}