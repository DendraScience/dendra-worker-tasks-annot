const chai = require('chai')
const feathers = require('@feathersjs/feathers')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const app = feathers()

const tm = require('@dendra-science/task-machine')
tm.configure({
  // logger: console
})

app.logger = console

const WEB_API_URL = 'http://api.preview.dendra.science/v2'

app.set('connections', {
  web: {
    // The Feathers app created by the worker service
    app: feathers().configure(restClient(WEB_API_URL).axios(axios)),

    // Used to create a task-level Feathers app with authentication
    auth: {
      email: process.env.WEB_API_AUTH_EMAIL,
      password: process.env.WEB_API_AUTH_PASS,
      strategy: 'local'
    },
    url: WEB_API_URL
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
