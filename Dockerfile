FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-mssqldb"]
COPY baton-mssqldb /