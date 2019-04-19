# Customer Maturity Analysis
This projects relies on New Relic REST API, Insights Query API and New Relic NRDB events to assess New Relic accounts' maturity levels.

Goal is to collect metrics that can be fully automated so web scrapping is out of the scope for the time being.

The account list can be consumed from local CSV files or a Google Sheet. The outcome metrics can also be written to different locations (local CSV files, Google Sheets, New Relic Insights)

## Preqrequisites
- [Create a new Google API Project](https://console.developers.google.com)
	- TODO step-by-step
	- ...
- [Install Homebrew](https://brew.sh/)
    - `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
- Install Python 3.X
    - `brew install python3`
- Install Python 3.x VirtualEnv package
    - `pip3 install --upgrade virtualenv`

## Install
- Download or git clone this repo into a working directory 
    - `git clone https://github.com/ThyWoof/newr-account-maturity`
- Setup a local python environment for the project
	- `cd <PATH_TO_CLONED_PROJECT>`
	- `virtualenv env`
	- `source env/bin/activate`
	- `pip install -r requirements.txt`
	 
## Launch 
- TODO
- ...

## Results
 - TODO
- ...