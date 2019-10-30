/**
 * Tests for build tasks
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

/*

  The following diagram represents this test case.

  NOTES:
    #1...#5 are annotations with intervals as shown.
    a...i represent points in time, e.g. begins_at.
    Legacy and Influx are two existing config instances.
    Processing should yield config instances [0]...[7].

     |
     |  +--------+- a -+--------+
  #1 |  |        |     |   #1   | [0]
     +- |        |- b -+--------+
        |        |     |        | [1]
     +- | Legacy |- c -+--------+
  #2 |  |        |     |   #2   | [2]
     +- |        |- d -+--------+
     |  |        |     |   #3   | [3]
  #3 |  +--------+- e -+--------+
     |  |        |     |   #3   | [4]
     +- |        |- f -+--------+
  #4 |  |        |     |   #4   | [5]
     +- |        |- g -+--------+
        | Influx |     |        | [6]
     +- |        |- h -+--------+
  #5 |  |        |     |   #5   | [7]
     |  |        |- i -+--------+
     |  |        |

 */

describe('build tasks', function() {
  this.timeout(60000)

  const now = new Date()
  const hostname = 'test-hostname-0'
  const hostParts = hostname.split('-')

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
          // NOTE: Deprecated in favor of consistent hashing
          // queue_group: 'dendra.annotationBuild.v1',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'dendra.annotationBuild.v1.req.{hostOrdinal}'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  const requestSubject = 'dendra.annotationBuild.v1.req.0'
  const testName = 'dendra-worker-tasks-annot UNIT_TEST'
  const evaluate = 'v = v * @{one} * @{obj.ten}'
  const evaluateRepl = 'v = v * 1 * 10'

  const id = {}
  const date = {
    a: '2013-05-07T23:10:00.000Z',
    b: '2013-05-08T00:10:00.000Z',
    c: '2018-05-09T17:10:00.000Z',
    d: '2018-05-09T18:10:00.000Z',
    e: '2018-05-09T19:10:00.000Z',
    f: '2018-05-09T20:10:00.000Z',
    g: '2018-05-09T21:10:00.000Z',
    h: '2018-05-10T21:10:00.000Z',
    i: '2200-02-02T00:00:00.000Z'
  }
  const webConnection = {}

  const authWebConnection = async () => {
    const cfg = main.app.get('connections').web
    const storageKey = (webConnection.storageKey = murmurHash3.x86.hash128(
      `TEST,${cfg.url}`
    ))
    const app = (webConnection.app = feathers()
      .configure(restClient(cfg.url).axios(axios))
      .configure(
        auth({
          storage: localStorage,
          storageKey
        })
      ))

    await app.authenticate(cfg.auth)
  }
  const removeDocuments = async (path, query) => {
    const res = await webConnection.app.service(path).find({ query })

    for (let doc of res.data) {
      await webConnection.app.service(path).remove(doc._id)
    }
  }
  const cleanup = async () => {
    await removeDocuments('/annotations', {
      description: testName
    })
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
  Object.defineProperty(model, 'hostname', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostname
  })
  Object.defineProperty(model, 'hostOrdinal', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostParts[hostParts.length - 1]
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

  before(async function() {
    await authWebConnection()
    await cleanup()

    id.org = (await webConnection.app.service('/organizations').create({
      name: testName
    }))._id

    id.station = (await webConnection.app.service('/stations').create({
      is_active: true,
      is_enabled: true,
      is_stationary: true,
      name: testName,
      organization_id: id.org,
      station_type: 'weather',
      time_zone: 'PST',
      utc_offset: -28800
    }))._id

    id.annotation1 = (await webConnection.app.service('/annotations').create({
      actions: [
        {
          exclude: true
        }
      ],
      intervals: [
        {
          // begins_at: '',
          ends_before: date.b
        }
      ],
      description: testName,
      is_enabled: true,
      organization_id: id.org,
      state: 'approved',
      station_ids: [id.station],
      title: `${testName} #1`
    }))._id

    id.annotation2 = (await webConnection.app.service('/annotations').create({
      actions: [
        {
          exclude: true
        }
      ],
      intervals: [
        {
          begins_at: date.c,
          ends_before: date.d
        }
      ],
      description: testName,
      is_enabled: true,
      organization_id: id.org,
      state: 'approved',
      station_ids: [id.station],
      title: `${testName} #2`
    }))._id

    id.annotation3 = (await webConnection.app.service('/annotations').create({
      actions: [
        {
          attrib: {
            obj: {
              ten: 10
            }
          }
        },
        {
          evaluate
        },
        {
          evaluate
        },
        {
          exclude: true
        },
        {
          flag: ['X', 'Y']
        }
      ],
      intervals: [
        {
          begins_at: date.d,
          ends_before: date.f
        }
      ],
      description: testName,
      is_enabled: true,
      organization_id: id.org,
      state: 'approved',
      station_ids: [id.station],
      title: `${testName} #3`
    }))._id

    id.annotation4 = (await webConnection.app.service('/annotations').create({
      actions: [
        {
          exclude: true
        }
      ],
      intervals: [
        {
          begins_at: date.f,
          ends_before: date.g
        }
      ],
      description: testName,
      is_enabled: true,
      organization_id: id.org,
      state: 'approved',
      station_ids: [id.station],
      title: `${testName} #4`
    }))._id

    id.annotation5 = (await webConnection.app.service('/annotations').create({
      actions: [
        {
          exclude: true
        }
      ],
      intervals: [
        {
          begins_at: date.h
          // ends_before: ''
        }
      ],
      description: testName,
      is_enabled: true,
      organization_id: id.org,
      state: 'approved',
      station_ids: [id.station],
      title: `${testName} #5`
    }))._id

    id.datastream = (await webConnection.app.service('/datastreams').create({
      attributes: {
        one: 1,
        obj: {
          ten: 1
        }
      },
      datapoints_config: [
        {
          begins_at: date.a,
          ends_before: date.e,
          params: {
            query: {
              compact: true,
              datastream_id: 3358,
              time_adjust: -28800
            }
          },
          path: '/legacy/datavalues-ucnrs'
        },
        {
          begins_at: date.e,
          // ends_before: '',
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
          path: '/influx/select'
        }
      ],
      description: testName,
      is_enabled: true,
      name: testName,
      organization_id: id.org,
      source_type: 'sensor',
      station_id: id.station,
      terms: {}
    }))._id
  })

  after(async function() {
    await cleanup()

    await Promise.all([
      model.private.stan
        ? new Promise((resolve, reject) => {
            model.private.stan.removeAllListeners()
            model.private.stan.once('close', resolve)
            model.private.stan.once('error', reject)
            model.private.stan.close()
          })
        : Promise.resolve()
    ])
  })

  it('should import', function() {
    tasks = require('../../../dist').build

    expect(tasks).to.have.property('sources')
  })

  it('should create machine', function() {
    machine = new tm.TaskMachine(model, tasks, {
      helpers: {
        logger: console
      },
      interval: 500
    })

    expect(machine).to.have.property('model')
  })

  it('should run', function() {
    model.scratch = {}

    return machine
      .clear()
      .start()
      .then(success => {
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
        expect(model).to.have.nested.property(
          'sources.dendra_annotationBuild_v1_req__hostOrdinal_.some_default',
          'default'
        )
      })
  })

  it('should find datapoints at start of config', function() {
    return webConnection.app
      .service('/datapoints')
      .find({
        query: {
          datastream_id: id.datastream,
          time: {
            $gte: '2013-05-07T23:00:00.000Z',
            $lt: '2013-05-07T23:30:00.000Z'
          },
          $limit: 10,
          $sort: {
            time: 1
          }
        }
      })
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(2)
          .and.deep.include.ordered.members([
            { t: '2013-05-07T23:10:00.000Z', o: -28800, v: 13.79 },
            { t: '2013-05-07T23:20:00.000Z', o: -28800, v: 13.86 }
          ])
      })
  })

  it('should find datapoints at middle of config', function() {
    return webConnection.app
      .service('/datapoints')
      .find({
        query: {
          datastream_id: id.datastream,
          time: {
            $gte: '2018-05-09T19:00:00.000Z',
            $lt: '2018-05-09T19:20:00.000Z'
          },
          $limit: 10,
          $sort: {
            time: 1
          }
        }
      })
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(2)
          .and.deep.include.ordered.members([
            { t: '2018-05-09T19:00:00.000Z', o: -28800, v: 17.25 },
            { t: '2018-05-09T19:10:00.000Z', o: -28800, v: 15.99 }
          ])
      })
  })

  it('should touch datastream using version_id', function() {
    return webConnection.app
      .service('/datastreams')
      .get(id.datastream)
      .then(doc => {
        return webConnection.app.service('/datastreams').patch(
          id.datastream,
          {
            $set: {
              source_type: 'sensor'
            }
          },
          {
            query: {
              version_id: doc.version_id
            }
          }
        )
      })
      .then(res => {
        expect(res).to.have.nested.property('_id', id.datastream)

        datastream = res
      })
  })

  it('should process processAnnotation request', function() {
    const msgStr = JSON.stringify({
      _id: 'process-annotation-1234',
      method: 'processAnnotation',
      spec: {
        annotation: {
          _id: '592f155746a1b867a114e0a0',
          datastream_ids: [id.datastream],
          is_enabled: true,
          organization_id: id.org,
          title: testName
        },
        annotation_before: {
          _id: '592f155746a1b867a114e0a0',
          is_enabled: false,
          organization_id: id.org,
          station_ids: [id.station],
          title: testName
        }
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 5 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify datastream patch after processAnnotation', function() {
    return webConnection.app
      .service('/datastreams')
      .get(id.datastream)
      .then(doc => {
        expect(doc).to.have.property('_id', datastream._id)
        expect(doc).to.have.property('version_id')
        expect(doc).to.not.have.property('version_id', datastream.version_id)

        datastream = doc
      })
  })

  it('should process assembleDatapointsConfig request', function() {
    const msgStr = JSON.stringify({
      _id: 'assemble-dayapoints-config-1234',
      method: 'assembleDatapointsConfig',
      spec: {
        datastream
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 5 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify datastream patch after assembleDatapointsConfig', function() {
    return webConnection.app
      .service('/datastreams')
      .get(id.datastream)
      .then(doc => {
        expect(doc).to.have.property('_id', datastream._id)

        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.begins_at',
          date.a
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.ends_before',
          date.b
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.0.actions.attrib'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.0.actions.evaluate'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.actions.exclude',
          true
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.0.actions.flag'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.annotation_ids.0',
          id.annotation1
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.path',
          '/legacy/datavalues-ucnrs'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.1.begins_at',
          date.b
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.1.ends_before',
          date.c
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.1.actions'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.1.annotation_ids'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.1.path',
          '/legacy/datavalues-ucnrs'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.2.begins_at',
          date.c
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.2.ends_before',
          date.d
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.2.actions.attrib'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.2.actions.evaluate'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.2.actions.exclude',
          true
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.2.actions.flag'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.2.annotation_ids.0',
          id.annotation2
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.2.path',
          '/legacy/datavalues-ucnrs'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.begins_at',
          date.d
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.ends_before',
          date.e
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.actions.attrib.obj.ten',
          10
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.actions.evaluate',
          `${evaluateRepl};${evaluateRepl}`
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.actions.exclude',
          true
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.actions.flag.0',
          'X'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.actions.flag.1',
          'Y'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.annotation_ids.0',
          id.annotation3
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.3.path',
          '/legacy/datavalues-ucnrs'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.begins_at',
          date.e
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.ends_before',
          date.f
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.actions.attrib.obj.ten',
          10
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.actions.evaluate',
          `${evaluateRepl};${evaluateRepl}`
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.actions.exclude',
          true
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.actions.flag.0',
          'X'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.actions.flag.1',
          'Y'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.annotation_ids.0',
          id.annotation3
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.4.path',
          '/influx/select'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.5.begins_at',
          date.f
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.5.ends_before',
          date.g
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.5.actions.attrib'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.5.actions.evaluate'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.5.actions.exclude',
          true
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.5.actions.flag'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.5.annotation_ids.0',
          id.annotation4
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.5.path',
          '/influx/select'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.6.begins_at',
          date.g
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.6.ends_before',
          date.h
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.6.actions'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.6.annotation_ids'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.6.path',
          '/influx/select'
        )

        expect(doc).to.have.nested.property(
          'datapoints_config_built.7.begins_at',
          date.h
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.7.ends_before',
          date.i
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.7.actions.attrib'
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.7.actions.evaluate'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.7.actions.exclude',
          true
        )
        expect(doc).to.not.have.nested.property(
          'datapoints_config_built.7.actions.flag'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.7.annotation_ids.0',
          id.annotation5
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.7.path',
          '/influx/select'
        )
      })
  })

  it('should NOT find datapoints at start of config', function() {
    return webConnection.app
      .service('/datapoints')
      .find({
        query: {
          datastream_id: id.datastream,
          time: {
            $gte: '2013-05-07T23:00:00.000Z',
            $lt: '2013-05-07T23:30:00.000Z'
          },
          $limit: 10,
          $sort: {
            time: 1
          }
        }
      })
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(0)
      })
  })

  it('should NOT find datapoints at middle of config', function() {
    return webConnection.app
      .service('/datapoints')
      .find({
        query: {
          datastream_id: id.datastream,
          time: {
            $gte: '2018-05-09T19:00:00.000Z',
            $lt: '2018-05-09T19:20:00.000Z'
          },
          $limit: 10,
          $sort: {
            time: 1
          }
        }
      })
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(0)
      })
  })
})
