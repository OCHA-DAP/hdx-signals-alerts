### Collector for HDX Signals Alerts
[![Build Status](https://github.com/OCHA-DAP/hdx-signals-alerts/actions/workflows/run-python-tests.yml/badge.svg)](https://github.com/OCHA-DAP/hdx-signals-alerts/actions/workflows/run-python-tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-signals-alerts/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-signals-alerts?branch=main)

This script connects to the [HDX Signals Project](link).


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-signals-alerts** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY
