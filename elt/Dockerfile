FROM python:3.8-slim

# Add PostgreSQL APT repository
RUN apt-get update && apt-get install -y wget gnupg2 lsb-release
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

# Install the specific version of PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client-16

# Copy the ELT script into the container
COPY elt_script.py .

# Set the command to run the ELT script
CMD [ "python", "elt_script.py" ]
