FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-sql-server"]
COPY baton-sql-server /