const chai = require('chai')
const feathers = require('@feathersjs/feathers')
const restClient = require('@feathersjs/rest-client')
const request = require('request')
const app = feathers()

const tm = require('@dendra-science/task-machine')
tm.configure({
  // logger: console
})

app.logger = console

const WEB_API_URL = 'http://preview.api.dendra.science/v1'

app.set('connections', {
  web: {
    app: feathers().configure(restClient(WEB_API_URL).request(request)),
    authenticate: {
      email: '',
      password: '',
      strategy: 'local'
    }
  }
})

app.set('clients', {
  stan: {
    client: 'test-annot-{key}',
    cluster: 'test-cluster',
    opts: {
      uri: 'http://localhost:4222'
    }
  }
})

global.assert = chai.assert
global.expect = chai.expect
global.main = {
  app
}
global.tm = tm
