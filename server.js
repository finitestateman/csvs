const express = require('express');
const fs = require('fs');
const path = require('path');
const app = express();
const PORT = 3000;

// Endpoint: /indicators/<filename>?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
app.get('/indicators/:filename', (req, res) => {
    const { filename } = req.params;
    const { startDate, endDate } = req.query;

    // 1. Validate inputs exist
    if (!startDate || !endDate) {
        return res.json([]); // Return empty if dates are missing
    }

    // 2. Construct file path
    const filePath = path.join(__dirname, 'jsons', 'indicators', `${filename}.json`);

    // 3. Check if file exists
    if (!fs.existsSync(filePath)) {
        console.log(`File not found: ${filePath}`);
        return res.json([]);
    }

    try {
        // 4. Read and Parse JSON
        const rawData = fs.readFileSync(filePath, 'utf-8');
        const jsonArray = JSON.parse(rawData);

        // 5. Helper function to normalize dates (strip time)
        const toMidnight = (dateStr) => {
            const date = new Date(dateStr);
            date.setHours(0, 0, 0, 0); // Ignore time, set to local midnight
            return date;
        };

        const start = toMidnight(startDate);
        const end = toMidnight(endDate);

        // 6. Filter
        const result = jsonArray.filter(item => {
            if (!item.dateTime) return false;
            
            const itemDate = toMidnight(item.dateTime);
            
            // Compare timestamps (inclusive)
            return itemDate.getTime() >= start.getTime() && 
                   itemDate.getTime() <= end.getTime();
        });

        res.json(result);

    } catch (err) {
        console.error("Error processing request:", err);
        res.json([]);
    }
});

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

