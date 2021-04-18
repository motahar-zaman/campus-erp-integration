FROM python:3.7
WORKDIR /opt/app

RUN python -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

# Add credentials on build
ARG SSH_PRIVATE_KEY
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh && echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa
ENV GIT_SSH_COMMAND "ssh -i /root/.ssh/id_rsa -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# Make sure your domain is accepted
RUN touch /root/.ssh/known_hosts
ARG SSH_KNOWN_HOST
RUN ssh-keyscan ${SSH_KNOWN_HOST} >> /root/.ssh/known_hosts

ARG REPO_URL_CC_LIBS
ARG VERSION_CC_LIBS
RUN git clone --branch ${VERSION_CC_LIBS} ${REPO_URL_CC_LIBS} /opt/marketplace-shared-lib/

ARG REPO_URL_CC_MODELS_PG
ARG VERSION_CC_MODELS_PG
RUN git clone --branch ${VERSION_CC_MODELS_PG} ${REPO_URL_CC_MODELS_PG} /opt/campuscom-shared-models/

ARG REPO_URL_CC_MODELS_MONGO
ARG VERSION_CC_MODELS_MONGO
RUN git clone --branch ${VERSION_CC_MODELS_MONGO} ${REPO_URL_CC_MODELS_MONGO} /opt/marketplace-shared-models/

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["python", "worker.py"]
