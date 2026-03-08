function transform(line) {
    var values = line.split(","); // Split the input line by commas (CSV format)
    if (values[0] === "TransactionDate") {
        return null; // Skip the header line
    }

    var obj = new Object();
    obj.TransactionDate = values[0];
    obj.SequenceNumber = parseInt(values[1]); // Convert to integer
    obj.ServiceProvider = values[2];
    obj.Location = values[3];
    obj.Type = values[4];
    obj.Service = values[5];
    obj.Discount = parseFloat(values[6]);
    obj.Amount = parseFloat(values[7]);
    obj.Balance = parseFloat(values[8]);

    // Convert the object to a JSON string
    var jsonString = JSON.stringify(obj);
    return jsonString;
}