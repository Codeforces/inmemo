mvn -Dfile.encoding=UTF-8 -DcreateChecksum=true --batch-mode clean source:jar javadoc:jar repository:bundle-create install %*
