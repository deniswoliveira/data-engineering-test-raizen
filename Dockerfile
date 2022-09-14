FROM python:3.9.14-slim
LABEL Maintainer="Denis Oliveira"
RUN apt-get update && apt-get install -y \
    && apt-get install libreoffice -y \
    && apt-get install -y locales \
    && localedef -i pt_BR -f UTF-8 pt_BR.UTF-8 \
    && pip install --upgrade pip \
    && pip install --upgrade setuptools

RUN groupadd -r raizen --gid=1280 && useradd -r -g raizen --uid=1280 --create-home --shell /bin/bash raizen
RUN chown -R raizen:raizen /opt/
USER raizen

WORKDIR /opt/app/
COPY ./src/requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

COPY --chown=raizen:raizen . .

RUN python ./src/generate_anp_fuel_sales.py

WORKDIR /opt/app/tests
RUN python -m pytest --doctest-modules \
    --junitxml=xunit-reports/xunit-result-all.xml 

WORKDIR /opt/trusted_data
ENTRYPOINT [ "/bin/bash"]