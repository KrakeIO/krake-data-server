Sequelize = require 'sequelize'

recordBody = 
  properties: 'hstore',
  pingedAt: Sequelize.DATE


module.exports = recordBody