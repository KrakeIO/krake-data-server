Sequelize = require 'sequelize'

recordBody = 
  properties: Sequelize.TEXT,
  pingedAt: Sequelize.DATE


module.exports = recordBody