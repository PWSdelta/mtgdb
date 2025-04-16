@app.route('/random-card-view')
def random_card_view():
    try:
        # Initialize MongoDB connection
        client = MongoClient('mongodb://localhost:27017/')
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # Use MongoDB's $sample aggregation to get a random document
        random_card = list(cards_collection.aggregate([
            {"$sample": {"size": 1}}
        ]))

        if not random_card:
            return "No cards found in the database", 404

        card = random_card[0]

        # Known image field names (modify these based on your actual data structure)
        image_fields = [
            'image_uris', 'image', 'card_faces', 'artwork', 'image_front', 'image_back',
            'small_image', 'normal_image', 'large_image', 'png', 'art_crop', 'border_crop'
        ]

        # Create HTML for the card
        html = ['<!DOCTYPE html>',
                '<html>',
                '<head>',
                '<title>MTG Card: {}</title>'.format(card.get('name', 'Random Card')),
                '<meta charset="UTF-8">',
                '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
                '<style>',
                '  body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }',
                '  .card { border: 1px solid #ddd; padding: 20px; border-radius: 8px; max-width: 800px; margin: 0 auto; background: white; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }',
                '  .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; }',
                '  .card-name { margin: 0; color: #444; flex: 1; }',
                '  .card-meta { text-align: right; flex: 1; }',
                '  .card-gallery { display: flex; flex-wrap: wrap; justify-content: center; gap: 20px; margin: 20px 0; }',
                '  .card-image { text-align: center; flex: 0 0 auto; }',
                '  .card-image img { max-width: 300px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.15); }',
                '  .card-image figcaption { margin-top: 8px; color: #666; }',
                '  .card-details { margin-top: 30px; }',
                '  .card-prop { margin: 12px 0; line-height: 1.5; }',
                '  .prop-name { font-weight: bold; display: inline-block; width: 130px; color: #555; }',
                '  .oracle-text { background: #f5f5f5; padding: 15px; border-radius: 8px; white-space: pre-line; }',
                '  .json-toggle { background: #eee; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; margin: 20px 0; }',
                '  .json-data { display: none; background: #f5f5f5; padding: 15px; border-radius: 8px; overflow: auto; }',
                '  .visible { display: block; }',
                '  .button { display: inline-block; background: #6a90b0; color: white; padding: 10px 20px; border-radius: 4px; text-decoration: none; margin-top: 20px; }',
                '  .button:hover { background: #507799; }',
                '</style>',
                '</head>',
                '<body>',
                '<div class="card">']

        # Card header with name and basic info
        html.append('<div class="card-header">')
        html.append(f'<h1 class="card-name">{card.get("name", "Unknown Card")}</h1>')
        html.append('<div class="card-meta">')
        if 'mana_cost' in card:
            html.append(f'<div><span class="prop-name">Mana:</span> {card["mana_cost"]}</div>')
        if 'type_line' in card:
            html.append(f'<div><span class="prop-name">Type:</span> {card["type_line"]}</div>')
        html.append('</div>')  # Close card-meta
        html.append('</div>')  # Close card-header

        # Add image gallery section
        html.append('<div class="card-gallery">')

        # Handle binary image data
        import base64

        # Process known image fields
        images_added = False
        for key, value in card.items():
            # Check if this is a known image field or contains binary data
            is_image_field = any(img_name in key.lower() for img_name in image_fields)
            is_binary = isinstance(value, bytes)

            if is_binary or is_image_field:
                images_added = True

                if is_binary:
                    # Direct binary data
                    mime_type = 'image/jpeg'  # Default assumption
                    if 'png' in key.lower():
                        mime_type = 'image/png'

                    base64_image = base64.b64encode(value).decode('utf-8')
                    img_src = f"data:{mime_type};base64,{base64_image}"

                    html.append('<figure class="card-image">')
                    html.append(f'<img src="{img_src}" alt="{key}" />')
                    html.append(f'<figcaption>{key.replace("_", " ").title()}</figcaption>')
                    html.append('</figure>')
                elif isinstance(value, dict) and any(isinstance(value.get(subkey), bytes) for subkey in value):
                    # Dictionary with binary fields
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, bytes):
                            mime_type = 'image/jpeg'
                            if 'png' in subkey.lower():
                                mime_type = 'image/png'

                            base64_image = base64.b64encode(subvalue).decode('utf-8')
                            img_src = f"data:{mime_type};base64,{base64_image}"

                            html.append('<figure class="card-image">')
                            html.append(f'<img src="{img_src}" alt="{key}/{subkey}" />')
                            html.append(f'<figcaption>{key}/{subkey}</figcaption>')
                            html.append('</figure>')

        if not images_added:
            html.append('<p>No images found for this card.</p>')

        html.append('</div>')  # Close card-gallery

        # Card details section
        html.append('<div class="card-details">')
        html.append('<h2>Card Details</h2>')

        # Show oracle text in a special format if available
        if 'oracle_text' in card:
            html.append('<div class="card-prop oracle-text">')
            html.append(f'{card["oracle_text"]}')
            html.append('</div>')

        # Important card properties
        important_props = ['rarity', 'set_name', 'flavor_text', 'power', 'toughness',
                           'colors', 'keywords', 'legalities', 'collector_number', 'artist']

        for prop in important_props:
            if prop in card:
                value = card[prop]
                if isinstance(value, list):
                    value = ', '.join(str(x) for x in value)
                elif isinstance(value, dict):
                    # Format dictionary values nicely
                    value = ', '.join(f"{k}: {v}" for k, v in value.items())
                html.append(
                    f'<div class="card-prop"><span class="prop-name">{prop.replace("_", " ").title()}:</span> {value}</div>')

        # Add a button to show/hide raw JSON data
        html.append('<button class="json-toggle" onclick="toggleJson()">Show/Hide Raw JSON Data</button>')

        # Convert card to a JSON-serializable format
        import json
        from bson import ObjectId

        class MongoJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, ObjectId):
                    return str(obj)
                elif isinstance(obj, bytes):
                    return "[binary data]"
                return super().default(obj)

        card_json = json.dumps(card, indent=2, cls=MongoJSONEncoder)

        html.append(f'<pre id="jsonData" class="json-data">{card_json}</pre>')

        # Add a button to get another random card
        html.append('<div style="text-align: center; margin-top: 30px;">')
        html.append('<a href="/random-card-view" class="button">Get Another Random Card</a>')
        html.append('</div>')

        html.append('</div>')  # Close card-details div
        html.append('</div>')  # Close card div

        # Add JavaScript for toggling JSON visibility
        html.append('<script>')
        html.append('function toggleJson() {')
        html.append('  var jsonElement = document.getElementById("jsonData");')
        html.append('  jsonElement.classList.toggle("visible");')
        html.append('}')
        html.append('</script>')

        html.append('</body></html>')

        return '\n'.join(html)

    except Exception as e:
        import traceback
        print(f"Error retrieving random card: {e}")
        print(traceback.format_exc())
        return f"Error: {str(e)}", 500

    finally:
        if 'client' in locals():
            client.close()