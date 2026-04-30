#!/bin/bash
export DATABASE_URL="postgresql://<username>:<password>@<host>:<port>/<database>?sslmode=require"
export PGRST_DB_ANON_ROLE=<your_db_role>
export PGRST_DB_SCHEMAS="gold"
./target/release/postrust
