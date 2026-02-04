console.log(js_data);

// Function to generate and insert HTML
function generateHTMLContent(data) {
    let html = '<ul>';
    data.forEach(item => {
        html += `<li>${item}</li>`;
    });
    html += '</ul>';

    // Insert the generated HTML into the container div
    const container = document.getElementById('content-container');
    if (container) {
        container.innerHTML = html; // Use innerHTML to insert HTML content
    }
}

// Call the function with the data
generateHTMLContent(js_data);