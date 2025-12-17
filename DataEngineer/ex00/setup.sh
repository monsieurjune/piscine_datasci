#!/usr/bin/env /bin/bash

# Set variables
env_file=.env

append_env() {
    echo $2=$3 >> $1
}

input_with_default() {
    local prompt="$1"
    local default="$2"
    local input=""

    # Prompt for normal input
    read -r -p "$prompt" input
    if [ -z "$input" ] && [ -n "$default" ]; then
        input="$default"
    fi

    echo "$input"
}

append_db() {
    POSTGRES_DB=$(input_with_default "POSTGRES_DB [default: piscineds]: " "piscineds")

    append_env $env_file POSTGRES_DB $POSTGRES_DB
}

append_username() {
    POSTGRES_USER=$(input_with_default "POSTGRES_USER [default: user42]: " "user42")

    append_env $env_file POSTGRES_USER $POSTGRES_USER
}

append_password() {
    # Enforcing 8 characters password
    while true; do
        read -s -p "POSTGRES_PASSWORD (At least 8 characters): " POSTGRES_PASSWORD
        echo ""

        if [ ${#POSTGRES_PASSWORD} -lt 8 ]; then
            echo "❌ Password must be at least 8 characters long. Please try again."
        else
            break
        fi
    done

    append_env $env_file POSTGRES_PASSWORD $POSTGRES_PASSWORD
}

append_email() {
    read -r -p "PGADMIN_DEFAULT_EMAIL: " PGADMIN_DEFAULT_EMAIL

    append_env $env_file PGADMIN_DEFAULT_EMAIL $PGADMIN_DEFAULT_EMAIL
}

append_pgadmin_password() {
    # Enforcing 8 characters password
    while true; do
        read -s -p "PGADMIN_DEFAULT_PASSWORD (At least 8 characters): " PGADMIN_DEFAULT_PASSWORD
        echo ""

        if [ ${#PGADMIN_DEFAULT_PASSWORD} -lt 8 ]; then
            echo "❌ Password must be at least 8 characters long. Please try again."
        else
            break
        fi
    done

    append_env $env_file PGADMIN_DEFAULT_PASSWORD $PGADMIN_DEFAULT_PASSWORD
}

# Exit on error
set -e
echo "Generating env..."

# Check env file
if [ -f $env_file ]; then
    echo "$env_file files have already been generated."
    return 0
fi

# Create env files
touch $env_file
chmod 600 $env_file

# Append env to $env_file
append_db
append_username
append_password

append_email
append_pgadmin_password
