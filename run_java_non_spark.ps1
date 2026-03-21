param(
    [string]$Dataset = "datasets/twitter_training_1GB_cleaned.csv",
    [string]$Output = ""
)

$javaCandidates = @(
    "C:\Program Files\Java\jdk-17",
    "C:\Program Files\Java\jdk-21",
    "C:\Program Files\Java\jdk-20",
    "C:\Program Files\Java\jdk-23"
) | Where-Object { Test-Path $_ }

if (-not $javaCandidates) {
    throw "No supported JDK found. Install JDK 17, 20, 21, or 23."
}

$env:JAVA_HOME = $javaCandidates[0]
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:SENTIMENT_DATASET = $Dataset

if ([string]::IsNullOrWhiteSpace($Output)) {
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($Dataset)
    $env:SENTIMENT_OUTPUT = "output/$($stem)_word_counts.csv"
} else {
    $env:SENTIMENT_OUTPUT = $Output
}

Write-Host "Using JAVA_HOME=$env:JAVA_HOME"
Write-Host "Dataset=$env:SENTIMENT_DATASET"
Write-Host "Output=$env:SENTIMENT_OUTPUT"

& mvn -q -DskipTests compile
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

& "$env:JAVA_HOME\bin\java.exe" "-cp" "target\classes" "ie.ucd.bigdata.NonSparkSentimentWordCount" $env:SENTIMENT_DATASET $env:SENTIMENT_OUTPUT
exit $LASTEXITCODE
