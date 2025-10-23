# VGU_CTCS

## Setup

The repo is designed to be run inside Github Codespace, but you can have a look in `setup.sh` to adapt the setup locally.

1. Create a codespace from this repository
2. Copy `.env.example` into `.env`, fill in the credential for yourself.
3. Download `CompanyX.bak` and place it in `dataset/`. Create the folder if it does not exist.
4. Run `setup.sh`
5. You are all set!

## ETL

Double check to make sure that the path to the shell script is correct.
Then add `etl/crontab_definition` into the user's crontab (`crontab -e etl/crontab_definition`)
Note that the job run on the first day of month.