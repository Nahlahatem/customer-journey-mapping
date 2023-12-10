#!/bin/bash

# Run each Python script in the background
python streamingdata.py &
python RecommendationEngine.py &
python UploadStreamingTransaction.py &
python UploadStreamingRecommendation.py &

# Keep the container running
tail -f /dev/null