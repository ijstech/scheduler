const CronParser = require('cron-parser');
const Uuid = require('uuid');
const Vm = require('@ijstech/vm');
const Url = require('url');
const https = require('https');
const http = require('http');

var Jobs = [];
var Options; 
var ProcessJobTimer;
var ModuleIdx = {};
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
        console.dir(err)
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
        console.dir(err);
        return;
    }
}

function runJob(job, module, script){        
    return new Promise(async function(resolve){        
        let vm = new Vm({
            org: job.org,
            logging: true,
            script: script,
            plugins: module.require || [],
            database: job.db,
            dbConfig: Options.db
        });
        try{
            await vm.eval(`
                (async function _untrusted() {
                    var result = await handleRequest()
                })`)
        }
        catch(err){            
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
    let module = ModuleIdx[(job.package + modulePath).toLowerCase()]    
    return module;
}
async function getJobScript(module){    
    let id = module.id;  
    module.require = module.require || [];  
    let result = await getModuleScript(UpdateServerConfig, id);        
    if (module.require.indexOf('@ijstech/pdm') < 0 && Array.isArray(result.require)){
        for (let i = 0; i < result.require.length; i ++){
            let item = result.require[i].toLowerCase();
            if (item.substr(item.length - 4) == '.pdm'){
                module.require.push('@ijstech/pdm');
                break;
            }
        }
    }
    return result.script;
}
function processJobs(){    
    return new Promise(async function(resolve){
        console.clear()
        console.log('##Pending jobs:')
        for (let i = 0; i < Jobs.length; i ++){            
            let job = Jobs[i];
            let now = new Date();       
            console.log(job.module +  ' ' + ((job.next.getTime() - now.getTime()) / 1000));
            if (now.getTime() >= job.next.getTime()){                                
                try{                         
                    let module = getModule(job);
                    if (!module)
                        return resolve();                
                    let script = await getJobScript(module);
                    console.log('Running ...')                    
                    await runJob(job, module, script);
                    job.next = CronParser.parseExpression(job.cron).next();
                }
                catch(err){                    
                    console.dir(err)
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
    catch(err){}    
    clearTimeout(ProcessJobTimer);
    ProcessJobTimer = null;
    ProcessJobTimer = setTimeout(start, 500);
}
module.exports = {
    init: async function(options){              
        if (options.jobs){          
            Options = options;      
            UpdateServerConfig = options.updateServer;
            let package = {}
            for (let p in options.package){
                try{
                    let pack = options.package[p];
                    if (pack.liveUpdate){
                        let code = await getModuleCode(UpdateServerConfig, pack.id)
                        let packData = JSON.parse(code);
                        for (let m in packData.modules){
                            let module = packData.modules[m];                        
                            let path = p.toLowerCase();
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
                }
                catch(err){
                    console.dir(err)
                }            
            }
            for (let i = 0; i < options.jobs.length; i ++){            
                let job = options.jobs[i];
                Jobs.push({
                    uid: Uuid.v4(),                
                    org: typeof(job.org)=='string'?options.org[job.org]:job.org,
                    cron: job.cron,
                    db: job.db,
                    package: job.package,
                    module: job.module,
                    next: CronParser.parseExpression(job.cron).next()
                })       
            }
            start();
        }    
    }
}