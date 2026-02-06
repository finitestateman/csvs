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

app.get('/markets/:filename', (req, res) => {
    const { filename } = req.params;
    const { startDate, endDate } = req.query; // 입력은 여전히 yyyy-MM-dd

    if (!startDate || !endDate) return res.json([]);

    // 폴더 경로: /jsons/markets/filename.json 라고 가정
    const filePath = path.join(__dirname, 'jsons', 'markets', `${filename}.json`);

    if (!fs.existsSync(filePath)) {
        console.log(`File not found: ${filePath}`);
        return res.json([]);
    }

    try {
        const jsonArray = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        const start = toMidnight(startDate);
        const end = toMidnight(endDate);

        const result = jsonArray.filter(item => {
            if (!item.Date) return false;

            // **핵심 로직: dd/MM/yyyy 파싱**
            // 예: "20/05/2024" -> ["20", "05", "2024"]
            const parts = item.Date.split('/');
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1; // JS 월은 0부터 시작하므로 -1
            const year = parseInt(parts[2], 10);

            // Date 객체 생성 및 시간 초기화
            const itemDate = new Date(year, month, day);
            itemDate.setHours(0, 0, 0, 0);

            // 범위 비교
            return itemDate.getTime() >= start.getTime() && 
                   itemDate.getTime() <= end.getTime();
        });

        res.json(result);

    } catch (err) {
        console.error("Error processing markets:", err);
        res.json([]);
    }
});

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

