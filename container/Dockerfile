FROM    crukcibioinformatics/alignment:4.0.5

LABEL   maintainer="Richard Bowers<richard.bowers@cruk.cam.ac.uk>"

ARG INSTALL_DIR=/opt
ARG BUILD_DIR=/var/tmp/alignment_software_build

ARG BOWTIE1=1.1.1
ARG JELLYFISH=2.3.0
ARG SALMON=1.8.0

ARG TAROPTS="--no-same-owner --no-same-permissions"

RUN dnf install -y python3 python3-urllib3

RUN mkdir -p ${INSTALL_DIR} ${BUILD_DIR}

# Bowtie 1
RUN cd ${BUILD_DIR}; \
    wget https://downloads.sourceforge.net/project/bowtie-bio/bowtie/${BOWTIE1}/bowtie-${BOWTIE1}-linux-x86_64.zip
RUN cd ${INSTALL_DIR}; \
    unzip -q ${BUILD_DIR}/bowtie-${BOWTIE1}-linux-x86_64.zip

# STAR - new version

ARG STAR=2.7.10a
RUN cd ${BUILD_DIR}; \
    wget -O STAR-${STAR}.tar.gz https://github.com/alexdobin/STAR/archive/${STAR}.tar.gz
RUN cd ${INSTALL_DIR}; \
    tar ${TAROPTS} -xzf ${BUILD_DIR}/STAR-${STAR}.tar.gz

# Salmon
RUN cd ${BUILD_DIR}; \
    wget -O salmon-${SALMON}.tar.gz https://github.com/COMBINE-lab/salmon/releases/download/v${SALMON}/salmon-${SALMON}_linux_x86_64.tar.gz
RUN cd ${INSTALL_DIR}; \
    tar ${TAROPTS} -xzf ${BUILD_DIR}/salmon-${SALMON}.tar.gz && \
    mv salmon-${SALMON}_linux_x86_64 salmon-${SALMON}

# UCSC Tools
RUN cd ${INSTALL_DIR}/ucsc_tools; \
    wget http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/genePredToGtf && \
    wget http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/gtfToGenePred && \
    chmod a+rx *

# Jellyfish
RUN cd ${BUILD_DIR}; \
    wget https://github.com/gmarcais/Jellyfish/releases/download/v${JELLYFISH}/jellyfish-${JELLYFISH}.tar.gz && \
    tar ${TAROPTS} -xzf jellyfish-${JELLYFISH}.tar.gz
RUN cd ${BUILD_DIR}/jellyfish-${JELLYFISH}; \
    export SAMTOOLS=${INSTALL_DIR}/samtools-${SAMTOOLS}/bin/samtools && \
    ./configure --prefix=${INSTALL_DIR}/jellyfish-${JELLYFISH} && \
    make && \
    make install

# This pipeline's Java programs.
COPY nf-referencebuilder.jar /opt

## Clean up
RUN cd / && rm -rf ${BUILD_DIR}
