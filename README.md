# Krake's Data Server

The system that interfaces the exporting of harvested data from Krake's web scraping engine
used in [Krake's Web Scraping Engine] (https://krake.io)

## Pre-requisities
- NodeJS — v0.8.8
- Coffee-Script
- PostGresql 9.2.2

## Setup

### Installation
```console
git clone git@github.com:KrakeIO/krake-toolkit.git
cd krake-toolkit
npm install
npm install coffee-script -g
```

### Configuration
Write the following lines in your ~/.bashrc file
```console
export KRAKE_PG_USERNAME='your_username'
export KRAKE_PG_PASSWORD='your_password'
export KRAKE_PG_HOST='your_host_location'
```

## Run server
```console
coffee krake_data_server.coffee
```