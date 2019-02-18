/**
 * Method to evaluate an annotation and trigger side-effects.
 */

const pick = require('lodash/pick')

async function processAnnotation (req, ctx) {
  // TODO: Add more logging
  // const { logger } = ctx
  // const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)

  // TODO: Finish this!

  return {}
}

module.exports = async (...args) => {
  try {
    return await processAnnotation(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
