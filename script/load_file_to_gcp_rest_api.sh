TOKEN="your access token"
BUCKET="your bucket name"

files=$(find . -type f -name '*.csv')
for file in $files; 
do
    filepath=$(echo "$file" | sed 's|^./||')
    # echo $filepath
    curl -X POST -T $filepath \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/csv" \
    "https://storage.googleapis.com/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$filepath"
done
