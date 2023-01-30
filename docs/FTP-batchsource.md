# FTP Batch Source


Description
-----------
Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines the source server
type, either FTP or SFTP.


Use Case
--------
This source is used whenever you need to read from an FTP or SFTP server.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Path:** Path to file(s) to be read. The path uses filename expansion (globbing) to read files.
Path is expected to be of the form prefix://username:password@hostname:port/path

**Format:** Format of the data to read.
The format must be one of 'blob', 'csv', 'delimited', 'json', 'text', 'tsv', or the
name of any format plugin that you have deployed to your environment. Note that FTP does
not support seeking in a file, so formats like avro and parquet cannot be used.
If the format is a macro, only the pre-packaged formats can be used.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'.

**Get Schema:** Auto-detects schema from file. Supported formats are: csv, delimited, tsv, blob and text.

Blob - is set by default as field named 'body' of type bytes.

Text - is set by default as two fields: 'body' of type bytes and 'offset' of type 'long'.

JSON - is not supported, user has to manually provide the output schema.

**Delimiter:** Delimiter to use when the format is 'delimited'. This will be ignored for other formats.

**Use First Row as Header:** Whether to use the first line of each file as the column headers. Supported formats are 'text', 'csv', 'tsv', 'delimited'.

**Enable Quoted Values** Whether to treat content between quotes as a value. This value will only be used if the format
is 'csv', 'tsv' or 'delimited'. For example, if this is set to true, a line that looks like `1, "a, b, c"` will output two fields.
The first field will have `1` as its value and the second will have `a, b, c` as its value. The quote characters will be trimmed.
The newline delimiter cannot be within quotes.

It also assumes the quotes are well enclosed. The left quote will match the first following quote right before the delimiter. If there is an
unenclosed quote, an error will occur.

**Regex Path Filter:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern.

**Allow Empty Input:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**File System Properties:** Additional properties to use with the InputFormat when reading the data.
