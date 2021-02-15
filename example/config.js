module.exports = {    
    plugin: {
        '@ijstech/scheduler': {
            jobs: [
                {
                    active: true,
                    cron: '*/4 * * * * *',
                    module: 'job/test.js'
                }
            ]
        }
    }
}