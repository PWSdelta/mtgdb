import requests
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database setup
Base = declarative_base()


class CardAnalysis(Base):
    __tablename__ = 'card_analyses'

    id = Column(Integer, primary_key=True)
    card_name = Column(String(255), nullable=False)
    topic = Column(String(100), nullable=False)
    analysis_text = Column(Text, nullable=False)
    analysis_type = Column(String(50), nullable=False)  # 'fun' or 'technical'
    temperature = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f"<CardAnalysis(card_name='{self.card_name}', type='{self.analysis_type}')>"


# Create database engine and session
engine = create_engine('sqlite:///mtg_analyses.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def store_analysis_in_db(card_name, topic, analysis_text, analysis_type, temperature):
    """
    Save the card analysis to the database
    
    Args:
        card_name (str): Name of the MTG card
        topic (str): Context for the analysis
        analysis_text (str): The generated analysis text
        analysis_type (str): 'fun' or 'technical'
        temperature (float): Temperature used for generation
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        session = Session()
        new_analysis = CardAnalysis(
            card_name=card_name,
            topic=topic,
            analysis_text=analysis_text,
            analysis_type=analysis_type,
            temperature=temperature
        )
        session.add(new_analysis)
        session.commit()
        session.close()
        return True
    except Exception as e:
        print(f"Database error: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return False


def analyze_card_dual_style(card_name, temperature=1.0, topic="commander_deck"):
    """
    Analyze a Magic: The Gathering card with two different analysis styles:
    1. Fun/Casual: Focused on entertainment value and casual gameplay experience
    2. Technical/Strategic: Focused on competitive aspects and optimal play

    Args:
        card_name (str): Name of the MTG card to analyze
        temperature (float): Temperature setting for LLM generation
        topic (str): Context for the analysis (e.g., "commander_deck")

    Returns:
        bool: True if analysis was successful, False otherwise
    """
    # Define analysis styles with their prompts and follow-up options
    analysis_styles = {
        "fun": {
            "name": "Fun/Casual Analysis",
            "description": "A playful analysis focusing on the entertainment value, table reactions, and casual gameplay experiences",
            "prompt_template": """
You are an enthusiastic and slightly irreverent Magic: The Gathering content creator analyzing '{card_name}' for {topic}.

Create a FUN and ENTERTAINING analysis that focuses on:
1. The "wow factor" and cool moments this card creates (7-10 on the Coolness Scale)
2. Funny stories or memorable plays this card enables
3. How this card makes people at the table FEEL when it's played
4. "Spicy" or unexpected ways to use this card that will surprise friends
5. The sheer joy/despair factor of drawing/facing this card

Write with humor, use casual language, and focus on the FUN aspects rather than pure optimization.
Include at least one joke or pun related to the card.

After your entertaining analysis, include this section:

---
## Fun Follow-up Options:

1. **Epic Stories**
   a. What are the most hilarious interactions with this card?
   b. Tell me about a game-winning moment this card could create
   c. What's the best "You did WHAT with that card?!" scenario?
   d. How would you use this to make the table groan or laugh?

2. **Social Impact**
   a. How do different player types react when this hits the table?
   b. What's the salt level when this card goes off?
   c. Will this make me friends or enemies at the LGS?
   d. How can I use this card to create memorable game moments?

3. **Flavor & Art**
   a. What's the flavor win with this card?
   b. Any funny combos that make thematic sense?
   c. How does the art reflect the card's gameplay?
   d. What would the card say if it could talk?

4. **Casual Deck Ideas**
   a. What's a fun (not necessarily optimal) deck using this card?
   b. What are some budget ways to make this card shine?
   c. What weird or janky combos work with this card?
   d. How can I build around this for maximum laughs?

5. **Table Politics**
   a. How can I use this card to forge alliances?
   b. What's the best way to avoid becoming the archenemy with this?
   c. How do I convince others this isn't a threat (even if it is)?
   d. What political leverage does this card give me?

6. **Fun Factor Rating**
   a. On a scale of "meh" to "chef's kiss," how satisfying is resolving this?
   b. What's the replay value of this card?
   c. Is this more fun to play WITH or AGAINST?
   d. What's the most entertaining thing to copy/steal/reanimate with this?
---

End with: "Want more fun insights? Choose an option (like '1a') or type 'switch' to see the technical analysis instead!"
""",
        },

        "technical": {
            "name": "Technical/Strategic Analysis",
            "description": "A detailed strategic analysis focusing on competitive viability, optimal play patterns, and meta positioning",
            "prompt_template": """
You are an elite Magic: The Gathering strategist and tournament player analyzing '{card_name}' for {topic}.

Create a TECHNICAL and STRATEGIC analysis that methodically examines:
1. Precise power level assessment with numerical ratings (scale 1-10) across different formats
2. Optimal strategic deployment and timing considerations
3. Mathematical evaluation of resource efficiency (mana/card advantage/tempo)
4. Specific synergies with top-tier cards and competitive archetypes
5. Technical positioning in the current competitive meta

Maintain an analytical tone, use precise terminology, and focus on OPTIMAL play patterns.
Include specific card interactions and concrete strategic advice backed by game theory principles.

After your technical analysis, include this section:

---
## Strategic Follow-up Options:

1. **Optimal Deployment**
   a. What is the mathematical analysis of this card's efficiency?
   b. What are the precise timing windows for maximum effect?
   c. How does the priority sequence work with this card?
   d. What are the stack interaction considerations?

2. **Meta Analysis**
   a. What is this card's expected win percentage contribution?
   b. Which tier 1 decks specifically benefit from this card?
   c. What is the current meta adaptation to counter this card?
   d. How has this card's performance data evolved over recent tournaments?

3. **Technical Synergies**
   a. What are the highest EV card combinations with this?
   b. Which competitive archetypes maximize this card's utility?
   c. What is the optimal ratio of support cards needed?
   d. What are the most efficient tutors to find this?

4. **Resource Optimization**
   a. What is the exact opportunity cost analysis of this card?
   b. How does this affect the optimal land count/curve?
   c. What is the tempo valuation of this card?
   d. How does this card perform in mathematical goldfishing models?

5. **Strategic Counterplay**
   a. What are the most efficient answers to this card?
   b. How do you play optimally when expecting this card?
   c. What is the correct sideboard strategy against this?
   d. What are the precise timing windows to disrupt this card?

6. **Format Analysis**
   a. What is the exact power level differential across formats?
   b. How does this card's mathematical win contribution vary by format?
   c. What is the optimal format for deploying this card competitively?
   d. What is the predicted future meta impact of this card?
---

End with: "Need more technical insights? Choose an option (like '1a') or type 'switch' to see the fun analysis instead!"
""",
        }
    }

    # Start with technical analysis by default
    current_style = "technical"
    initial_analysis_saved = False

    while True:
        # Get the current style details
        style = analysis_styles[current_style]

        # Generate the analysis using the template for the current style
        prompt = style["prompt_template"].format(card_name=card_name, topic=topic)

        print(f"\n=== Generating {style['name']} for {card_name} ===\n")

        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "gemma3:4b",
                    "prompt": prompt,
                    "temperature": temperature,
                    "stream": False
                },
                timeout=451
            )

            if response.status_code == 200:
                analysis = response.json().get("response", "")
                print(analysis)

                # Save to database if this is the first analysis
                if not initial_analysis_saved:
                    save_success = store_analysis_in_db(
                        card_name=card_name,
                        topic=topic,
                        analysis_text=analysis,
                        analysis_type=current_style,
                        temperature=temperature
                    )

                    if save_success:
                        print("\nAnalysis successfully saved to database.")
                        initial_analysis_saved = True
                    else:
                        print("\nWarning: Failed to save analysis to database.")
            else:
                print(f"Error: Failed to generate analysis (status code: {response.status_code})")
                return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

        # Handle user input for follow-ups or switching styles
        while True:
            user_choice = input("\nYour choice (or 'switch' to change styles, 'exit' to end): ").strip().lower()

            if user_choice == "exit":
                print("Analysis complete.")
                return True

            if user_choice == "switch":
                # Toggle between fun and technical styles
                current_style = "technical" if current_style == "fun" else "fun"
                break  # Break the inner loop to regenerate with new style

            # Handle follow-up questions within the current style
            if len(user_choice) >= 2 and user_choice[0] in "123456" and user_choice[1] in "abcd":
                # Parse the category and option
                category_num = user_choice[0]
                option_letter = user_choice[1]

                # Generate appropriate follow-up prompt based on the current style
                if current_style == "fun":
                    followup_categories = {
                        "1": "Epic Stories",
                        "2": "Social Impact",
                        "3": "Flavor & Art",
                        "4": "Casual Deck Ideas",
                        "5": "Table Politics",
                        "6": "Fun Factor Rating"
                    }

                    followup_prompt = f"""
You're continuing your FUN analysis of '{card_name}' with a focus on {followup_categories[category_num]}, specifically option {option_letter}.

Be entertaining, humorous, and focus on the casual/social gameplay experience. Include:
- Fun anecdotes or hypothetical scenarios
- Player reactions and table dynamics
- Creative and unexpected uses
- Flavorful connections and theme

Keep your tone light and engaging! End with a reminder that they can ask about another fun aspect or type 'switch' to see the technical analysis.
"""
                else:  # technical style
                    followup_categories = {
                        "1": "Optimal Deployment",
                        "2": "Meta Analysis",
                        "3": "Technical Synergies",
                        "4": "Resource Optimization",
                        "5": "Strategic Counterplay",
                        "6": "Format Analysis"
                    }

                    followup_prompt = f"""
You're continuing your TECHNICAL analysis of '{card_name}' with a focus on {followup_categories[category_num]}, specifically option {option_letter}.

Be precise, analytical, and focus on competitive optimization. Include:
- Specific card interactions and percentages
- Mathematical evaluations where relevant
- Optimal sequencing and timing considerations
- Strategic positioning against the meta

Maintain a technical, data-driven tone. End with a reminder that they can ask about another technical aspect or type 'switch' to see the fun analysis.
"""

                # Generate the follow-up response
                try:
                    response = requests.post(
                        "http://localhost:11434/api/generate",
                        json={
                            "model": "gemma3:4b",
                            "prompt": followup_prompt,
                            "temperature": temperature,
                            "stream": False
                        },
                        timeout=451
                    )

                    if response.status_code == 200:
                        followup_analysis = response.json().get("response", "")
                        print(f"\n{followup_analysis}")

                        # Option: Save follow-up analysis to database as well
                        # store_analysis_in_db(card_name, topic, followup_analysis, f"{current_style}_followup_{user_choice}", temperature)
                    else:
                        print(f"Error: Failed to generate follow-up response (status code: {response.status_code})")
                except Exception as e:
                    print(f"Error: {str(e)}")
            else:
                print("Invalid choice. Please enter a number+letter option (like '1a'), 'switch', or 'exit'.")