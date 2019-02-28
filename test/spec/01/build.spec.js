/**
 * Tests for build tasks
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const request = require('request')
const murmurHash3 = require('murmurhash3js')

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
  const skipText = 'DENDRA_SKIP_ANNOT_BUILD'
  const testName = 'dendra-worker-tasks-annot UNIT_TEST'

  const id = {}
  const webConnection = {}

  const authWebConnection = async () => {
    const cfg = main.app.get('connections').web
    const storageKey = webConnection.storageKey = murmurHash3.x86.hash128(`TEST,${cfg.url}`)
    const app = webConnection.app = feathers()
      .configure(restClient(cfg.url).request(request))
      .configure(auth({
        storage: localStorage,
        storageKey
      }))

    await app.authenticate(cfg.auth)
  }
  const removeDocuments = async (path, query) => {
    const res = await webConnection.app.service(path).find({ query })

    for (let doc of res.data) {
      await webConnection.app.service(path).remove(doc._id)
    }
  }
  const cleanup = async () => {
    await removeDocuments('/datastreams', {
      description: testName
    })
    await removeDocuments('/stations', {
      name: testName
    })
    await removeDocuments('/organizations', {
      name: testName
    })
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
  let datastream

  before(async function () {
    await authWebConnection()
    await cleanup()

    id.org = (await webConnection.app.service('/organizations').create({
      name: testName
    }))._id

    id.station = (await webConnection.app.service('/stations').create({
      enabled: true,
      is_active: true,
      is_stationary: true,
      name: testName,
      organization_id: id.org,
      station_type: 'weather',
      time_zone: 'PST',
      utc_offset: -28800
    }))._id

    id.datastream = (await webConnection.app.service('/datastreams').create({
      enabled: true,
      name: `${testName} ${skipText}`,
      datapoints_config: [
        {
          begins_at: '2013-05-07T23:10:00.000Z',
          params: {
            query: {
              compact: true,
              datastream_id: 3358,
              time_adjust: -28800
            }
          },
          path: '/legacy/datavalues-ucnrs',
          ends_before: '2018-05-09T19:10:00.000Z'
        },
        {
          params: {
            query: {
              api: 'ucnrs',
              db: 'station_ucac_angelo',
              fc: 'source_tenmin',
              sc: '"time", "TC_C_10_Avg"',
              utc_offset: -28800,
              coalesce: false
            }
          },
          begins_at: '2018-05-09T19:10:00.000Z',
          path: '/influx/select'
        }
      ],
      description: testName,
      organization_id: id.org,
      source_type: 'sensor',
      station_id: id.station
    }))._id
  })

  after(async function () {
    await cleanup()

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

  it('should find datapoints at start of config', function () {
    return webConnection.app.service('/datapoints').find({ query: {
      datastream_id: id.datastream,
      time: {
        $gte: '2013-05-07T23:00:00.000Z',
        $lt: '2013-05-07T23:30:00.000Z'
      },
      $limit: 10,
      $sort: {
        time: 1
      }
    } }).then(res => {
      expect(res).to.have.property('data').lengthOf(2).and.deep.include.ordered.members([
        { t: '2013-05-07T23:10:00.000Z', o: -28800, v: 13.79 },
        { t: '2013-05-07T23:20:00.000Z', o: -28800, v: 13.86 }
      ])
    })
  })

  it('should find datapoints at middle of config', function () {
    return webConnection.app.service('/datapoints').find({ query: {
      datastream_id: id.datastream,
      time: {
        $gte: '2018-05-09T19:00:00.000Z',
        $lt: '2018-05-09T19:20:00.000Z'
      },
      $limit: 10,
      $sort: {
        time: 1
      }
    } }).then(res => {
      expect(res).to.have.property('data').lengthOf(2).and.deep.include.ordered.members([
        { t: '2018-05-09T19:00:00.000Z', o: -28800, v: 17.25 },
        { t: '2018-05-09T19:10:00.000Z', o: -28800, v: 15.99 }
      ])
    })
  })

  it('should touch datastream using version_id', function () {
    return webConnection.app.service('/datastreams').get(id.datastream).then(doc => {
      return webConnection.app.service('/datastreams').patch(null, {}, { query: {
        _id: id.datastream,
        version_id: doc.version_id
      } })
    }).then(res => {
      expect(res).to.have.nested.property('0._id', id.datastream)

      datastream = res[0]
    })
  })

  it('should process processAnnotation request', function () {
    const msgStr = JSON.stringify({
      _id: 'process-annotation-1234',
      method: 'processAnnotation',
      spec: {
        annotation: {
          _id: '592f155746a1b867a114e0a0',
          datastream_ids: [id.datastream],
          enabled: true,
          organization_id: id.org,
          title: `${testName} ${skipText}`
        },
        annotation_before: {
          _id: '592f155746a1b867a114e0a0',
          enabled: false,
          organization_id: id.org,
          station_ids: [id.station],
          title: `${testName} ${skipText}`
        }
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) => err ? reject(err) : resolve(guid))
    })
  })

  it('should wait for 5 seconds', function () {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify datastream patch after processAnnotation', function () {
    return webConnection.app.service('/datastreams').get(id.datastream).then(doc => {
      expect(doc).to.have.property('_id', datastream._id)
      expect(doc).to.not.have.property('version_id', datastream.version_id)

      datastream = doc
    })
  })

  it.skip('should process assembleDatapointsConfig request', function () {
    const msgStr = JSON.stringify({
      _id: 'assemble-dayapoints-config-1234',
      method: 'assembleDatapointsConfig',
      spec: {
        datastream
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) => err ? reject(err) : resolve(guid))
    })
  })

  it.skip('should wait for 5 seconds', function () {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })
})
