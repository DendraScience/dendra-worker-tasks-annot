/**
 * Tests for build tasks
 */

describe('build tasks', function () {
  this.timeout(60000)

  const now = new Date()
  const model = {
    props: {},
    state: {
      _id: 'taskMachine-build-current',
      source_defaults: {
        some_default: 'default'
      },
      sources: [
        {
          description: 'Build annotations based on a method',
          queue_group: 'dendra.annotationBuild.v1',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'dendra.annotationBuild.v1.req'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  const requestSubject = 'dendra.annotationBuild.v1.req'

  const processAnnotationReq = {
    _id: 'process-annotation-1234',
    auth_info: {
      jwt: 'TOKEN'
    },
    method: 'processAnnotation',
    spec: {
      annotation: {}
    }
  }

  Object.defineProperty(model, '$app', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: main.app
  })
  Object.defineProperty(model, 'key', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: 'build'
  })
  Object.defineProperty(model, 'private', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: {}
  })

  let tasks
  let machine

  after(async function () {
    await Promise.all([
      model.private.stan ? new Promise((resolve, reject) => {
        model.private.stan.removeAllListeners()
        model.private.stan.once('close', resolve)
        model.private.stan.once('error', reject)
        model.private.stan.close()
      }) : Promise.resolve()
    ])
  })

  it('should import', function () {
    tasks = require('../../../dist').build

    expect(tasks).to.have.property('sources')
  })

  it('should create machine', function () {
    machine = new tm.TaskMachine(model, tasks, {
      helpers: {
        logger: console
      },
      interval: 500
    })

    expect(machine).to.have.property('model')
  })

  it('should run', function () {
    model.scratch = {}

    return machine.clear().start().then(success => {
      /* eslint-disable-next-line no-unused-expressions */
      expect(success).to.be.true

      // Verify task state
      expect(model).to.have.property('sourcesReady', true)
      expect(model).to.have.property('stanCheckReady', false)
      expect(model).to.have.property('stanCloseReady', false)
      expect(model).to.have.property('stanReady', true)
      expect(model).to.have.property('subscriptionsReady', true)
      expect(model).to.have.property('versionTsReady', false)

      // Check for defaults
      expect(model).to.have.nested.property('sources.dendra_annotationBuild_v1_req.some_default', 'default')
    })
  })

  it('should process processAnnotation request', function () {
    const msgStr = JSON.stringify(processAnnotationReq)

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) => err ? reject(err) : resolve(guid))
    })
  })

  it('should wait for 5 seconds', function () {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })
})
