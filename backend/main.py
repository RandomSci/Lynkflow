import os
import hashlib
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
from dotenv import load_dotenv
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial

load_dotenv()

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "EXAVITQu4vr4xnSDxMaL")
AUDIO_CACHE_DIR = Path(__file__).parent.parent / "frontend" / "static" / "audio"
AUDIO_CACHE_DIR.mkdir(parents=True, exist_ok=True)

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_CALLER_ID = os.getenv("TWILIO_CALLER_ID")
TWILIO_TWIML_APP_SID = os.getenv("TWILIO_TWIML_APP_SID")
GOOGLE_SHEETS_API_KEY = os.getenv("GOOGLE_SHEETS_API_KEY")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=Path(__file__).parent.parent / "frontend" / "static"), name="static")


class TTSRequest(BaseModel):
    text: str
    section_id: str


@app.get("/")
async def root():
    return FileResponse(Path(__file__).parent.parent / "frontend" / "index.html")


@app.post("/api/tts")
async def generate_tts(req: TTSRequest):
    if not ELEVENLABS_API_KEY:
        raise HTTPException(status_code=500, detail="ELEVENLABS_API_KEY not set in .env")

    cache_key = hashlib.md5(f"{req.section_id}:{req.text}".encode()).hexdigest()
    audio_path = AUDIO_CACHE_DIR / f"{cache_key}.mp3"

    if audio_path.exists():
        return JSONResponse({"url": f"/static/audio/{cache_key}.mp3", "cached": True})

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={
                "text": req.text,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {"stability": 0.4, "similarity_boost": 0.8, "style": 0.2, "use_speaker_boost": True},
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"ElevenLabs error: {resp.text}")

    audio_path.write_bytes(resp.content)
    return JSONResponse({"url": f"/static/audio/{cache_key}.mp3", "cached": False})


@app.get("/api/twilio/token")
async def get_twilio_token():
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_TWIML_APP_SID]):
        raise HTTPException(status_code=500, detail="Twilio credentials not set in .env")
    token = AccessToken(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_TWIML_APP_SID, identity="odelyn")
    grant = VoiceGrant(outgoing_application_sid=TWILIO_TWIML_APP_SID, incoming_allow=False)
    token.add_grant(grant)
    return JSONResponse({"token": token.to_jwt()})


@app.post("/api/twilio/voice")
async def twiml_voice(To: str = ""):
    response = VoiceResponse()
    dial = Dial(caller_id=TWILIO_CALLER_ID)
    dial.number(To)
    response.append(dial)
    from fastapi.responses import Response
    return Response(content=str(response), media_type="application/xml")


@app.get("/api/leads")
async def get_leads():
    if not GOOGLE_SHEETS_API_KEY or not GOOGLE_SHEET_ID:
        raise HTTPException(status_code=500, detail="Google Sheets credentials not set in .env")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1?key={GOOGLE_SHEETS_API_KEY}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Sheets error: {resp.text}")
    data = resp.json()
    rows = data.get("values", [])
    if len(rows) < 2:
        return JSONResponse([])
    headers = rows[0]
    leads = []
    for row in rows[1:]:
        lead = {}
        for i, h in enumerate(headers):
            lead[h.strip()] = row[i].strip() if i < len(row) else ""
        if lead.get("Phone"):
            leads.append(lead)
    return JSONResponse(leads)


@app.get("/api/modules")
async def get_modules():
    modules = [
        {
            "id": "greeting",
            "title": "Opening & Greeting",
            "icon": "👋",
            "explanation": "The first 5 seconds decide everything. A plumber gets sales calls all day — your opening has to sound human, confident, and different from every other call they've received. Do not rush. Do not sound like you're reading. The goal of the greeting is simply to earn 60 more seconds.",
            "tips": [
                "Smile before you dial — they can genuinely hear it in your voice",
                "Say their business name in the first sentence — it snaps their attention",
                "Never open with 'How are you today?' — it screams telemarketer immediately",
                "If they sound distracted or annoyed, slow down — don't speed up",
                "Keep your energy warm and steady, not excited or robotic",
            ],
            "script": "Hi, is this [Business Name]? Great — my name is Odelyn, I'm calling from Lynkflow. We help plumbing businesses make sure they never miss a customer call — even when they're in the middle of a job or it's the middle of the night. I just need about 60 seconds of your time. Is now an okay moment?",
            "sample_label": "Hear the opening",
        },
        {
            "id": "gatekeeper",
            "title": "Getting Past the Gatekeeper",
            "icon": "🚪",
            "explanation": "A gatekeeper is anyone who picks up the phone that isn't the owner — a receptionist, an office assistant, a spouse, an employee. They are not the decision maker. Do not pitch them. Your only job here is to get to the owner politely and confidently. Never lie, never be aggressive, never over-explain.",
            "tips": [
                "Sound like you belong — confident, not uncertain",
                "Don't over-explain what you're calling about to the gatekeeper",
                "Use the owner's first name if you have it — it signals familiarity",
                "If they ask what it's about, keep it vague but professional",
                "If the owner is truly unavailable, get a specific callback time — not 'try again later'",
            ],
            "script": "Hi there! Is the owner available? ... [If asked what it's about]: I'm calling about their phone system — it'll just take a minute. ... [If owner is unavailable]: No worries at all. What's the best time to reach them today — is morning or afternoon better? ... [When you reach the owner]: Hi! My name is Odelyn from Lynkflow — your team said you'd be the right person to speak with. I'll be quick, I promise.",
            "sample_label": "Hear gatekeeper navigation",
        },
        {
            "id": "dm_check",
            "title": "Decision Maker Check",
            "icon": "🎯",
            "explanation": "Even if someone picks up and seems engaged, always confirm they're the actual decision maker before launching into your pitch. Small plumbing shops often have employees or family members answering. Pitching the wrong person means your time is wasted and the message will almost never get passed on correctly.",
            "tips": [
                "Ask directly but warmly — it's not rude, it's professional",
                "If they say 'I can pass on the message' — that's a no. Get to the owner",
                "If they say 'I handle that too' — ask 'Are you the owner or manager?' to confirm",
                "Don't pitch until you've confirmed",
            ],
            "script": "Before I go further — are you the owner of the business, or the person who makes decisions on tools and services? ... [If yes]: Perfect, you're exactly who I need. ... [If no]: Got it — no problem at all. Is the owner available right now, or what's the best time I can reach them directly?",
            "sample_label": "Hear the DM check",
        },
        {
            "id": "pitch",
            "title": "The Core Pitch",
            "icon": "⚡",
            "explanation": "This is your 45-second window. Lead with the pain — missed calls cost real money. Then drop the solution. Do not over-explain the technology. Plumbers do not care how it's built. They care that they stop losing jobs to missed calls. Be punchy. Pause after your hook. Let it land.",
            "tips": [
                "Lead with the problem, not the product — make them feel the pain first",
                "Use specific scenarios — '2am', 'three customers at once', 'elbow-deep in a drain'",
                "Don't say 'AI' too early — say 'smart answering system' first if they seem traditional",
                "Pause after 'walking straight to your competitor' — let that sit for a second",
                "Keep it under 50 seconds. If you're going longer, you're over-explaining",
            ],
            "script": "Here's the thing — every time your phone rings and you can't get to it, that's a potential job walking straight to your competitor. Most people don't leave voicemails, and they don't call back. Lynkflow gives your business a 24/7 answering system that picks up every single call — even if three people call at the exact same time. It handles the conversation, finds out what the customer needs, books appointments straight into your calendar, and sends you a full summary by text or email right away. You never miss another lead. Not at 2am. Not on weekends. Not when you're knee-deep in a job. And it costs a fraction of what a receptionist would.",
            "sample_label": "Hear the full pitch",
        },
        {
            "id": "objections",
            "title": "Handling Objections",
            "icon": "🛡️",
            "explanation": "Objections are not rejections — they are questions in disguise. When someone says 'I'm not interested,' they usually mean 'I don't see the value yet.' Your job is to stay calm, acknowledge what they said, and redirect. Never argue. Never panic. Never raise your voice. Three solid objections in a row with no opening = disposition NI and move on.",
            "tips": [
                "Let them finish completely before you respond — interrupting kills the call",
                "Acknowledge first ('That makes sense', 'I completely understand') before redirecting",
                "Match their energy — calm caller, be calm. Direct caller, be direct",
                "Never read your rebuttal robotically — make it sound like your own words",
                "After three objections with no opening, let go gracefully. Don't push",
            ],
            "subsections": [
                {
                    "id": "obj_not_interested",
                    "label": "Not Interested",
                    "trigger": "I'm not interested / We're good / No thanks",
                    "script": "That's completely fair — I hear that a lot, honestly. Most owners I speak with said the same thing until I asked them one question: when you're out on a job and your phone rings, what actually happens to that call right now? ... [Let them answer] ... That's exactly the gap we close. It's not about replacing anything you have — it's about making sure zero calls slip through when you're unavailable. Would it be worth 30 more seconds to hear how it works?",
                    "second_objection": "I totally respect that. Last thing I'll say — on average, a missed plumbing call is worth anywhere from $200 to $2,000 depending on the job. If this saves you even one call a month you would've lost, it pays for itself immediately. That's all I wanted to put in front of you.",
                    "third_objection": "Understood — I won't take any more of your time. I appreciate you listening. Have a great rest of your day.",
                },
                {
                    "id": "obj_busy",
                    "label": "I'm Busy / Bad Time",
                    "trigger": "I'm busy right now / Not a good time / Call me back",
                    "script": "I completely understand — I'll be really quick. 30 seconds, and if it's not relevant I'll let you go immediately. The reason I'm calling is actually about what happens to your calls when you're this busy — which sounds like it's pretty often. Can I have just 30 seconds?",
                    "second_objection": "Of course — I don't want to hold you up. What's the best time to reach you today? Morning or afternoon?",
                    "third_objection": "No problem at all. I'll try you another time. Thanks for picking up.",
                },
                {
                    "id": "obj_already_have",
                    "label": "Already Have Something",
                    "trigger": "We have voicemail / We have a receptionist / We already use an answering service",
                    "script": "That's great — glad you have something in place. Quick question though — does it answer at 2am? And if two customers call at the exact same time, does it handle both of them simultaneously? Most services miss at least one of those. Lynkflow doesn't. And after every single call, you get a full summary of what was said sent straight to your phone. No call slips through, ever.",
                    "second_objection": "Makes sense. What if I told you most of our clients kept what they had and just added Lynkflow as the overflow — so nothing falls through the cracks after hours or during busy periods? It's not an either-or.",
                    "third_objection": "Understood, I appreciate your time. If that ever changes, we're here.",
                },
                {
                    "id": "obj_cost",
                    "label": "How Much Does It Cost",
                    "trigger": "What's the price? / How much is it? / That sounds expensive",
                    "script": "It's a one-time setup fee between $300 and $500 depending on how we customize it for your business, and then $100 to $150 a month after that. To put that in perspective — one missed call from a decent job is usually $500 to $2,000. If Lynkflow saves you even two calls a month you would've missed, it's paid for itself several times over. Most of our clients see that within the first week.",
                    "second_objection": "I hear you on the cost — and I want to be straight with you, it's not the cheapest option out there. But it's also not a voicemail box. It's a live system that talks to your customers, qualifies them, and books them in. The value is in never losing a lead again.",
                    "third_objection": "Fair enough — I appreciate the honest conversation. If budget opens up down the line, we'd love to work with you.",
                },
                {
                    "id": "obj_think_about",
                    "label": "Need to Think About It",
                    "trigger": "Let me think about it / I'll get back to you / Send me an email",
                    "script": "Of course — I respect that. Can I ask what part you want to think through? Sometimes I can answer it right now and save you the time. Is it the price, how it actually works, or something else? ... [After they answer]: That makes sense. How about this — I'll follow up with you on [specific day]. Does [morning or afternoon] work better for a quick 5-minute call?",
                    "second_objection": "Totally fine. I'll send you a quick summary so you have something to reference. What's the best email to send it to?",
                    "third_objection": "No problem at all. I'll leave it with you — and if you have any questions, just reach out. Have a good one.",
                },
                {
                    "id": "obj_scam",
                    "label": "Is This a Scam / Legit?",
                    "trigger": "How do I know this is real? / Sounds like a scam / I don't know you",
                    "script": "That's a completely valid concern — you should be skeptical of random calls, honestly. My name is Odelyn, I'm with Lynkflow. We're a small agency that builds AI phone systems specifically for trade businesses like plumbers. I'm not asking for any payment or information right now — I just want to show you what the system does. You can look us up at lynkflow.com. Would it help if I sent you something in writing first so you can check us out before deciding anything?",
                    "second_objection": "Completely fair. No pressure at all — take your time to look us up. If you want to connect after, I'm happy to walk you through a demo with no commitment.",
                    "third_objection": "Understood. Thanks for being upfront. Have a good day.",
                },
                {
                    "id": "obj_no_need",
                    "label": "We Don't Miss Calls",
                    "trigger": "We always answer / I never miss calls / We're on top of it",
                    "script": "That's awesome — and I believe you during business hours. But what about 9pm on a Saturday when someone's pipe bursts? Or when you're on a job and two people call at the exact same time? Those are the moments we cover. Even the most on-top-of-it businesses have gaps after hours. Lynkflow fills those gaps without you having to do anything differently.",
                    "second_objection": "Fair enough — if you're genuinely covered around the clock with zero gaps, then honestly, you might not need us. Not every business does. I just wanted to make sure you'd heard about it.",
                    "third_objection": "I appreciate that. Good luck with everything — sounds like you're running a tight operation.",
                },
            ],
            "sample_label": "Hear objection handling",
        },
        {
            "id": "faqs",
            "title": "Plumber FAQs",
            "icon": "❓",
            "explanation": "These are real questions plumbers ask during calls. Know every answer cold. You should be able to answer any of these without hesitation — hesitation kills trust. Study these until they feel like your own words, not a script.",
            "faqs": [
                {
                    "id": "faq_real_person",
                    "question": "Are you a real person or is this a robot?",
                    "answer": "I'm a real person — my name is Odelyn. I work for Lynkflow and I'm calling because we help plumbing businesses like yours handle their calls more efficiently. The system I'm offering is automated, but I'm very much a real person talking to you right now.",
                },
                {
                    "id": "faq_location",
                    "question": "Where are you located? / Where are you calling from?",
                    "answer": "I'm based in the Philippines — we're a remote team that works with businesses across the US. A lot of companies work with offshore teams these days because it keeps costs down without sacrificing quality. The system itself is built and managed to US standards, and everything runs in the US for your customers.",
                },
                {
                    "id": "faq_accent",
                    "question": "I can't understand you / You have an accent",
                    "answer": "I completely understand — I appreciate you letting me know. I'll slow down a bit. [Slow your speech, speak clearly.] Is that better? I want to make sure you get the full picture before you decide anything.",
                },
                {
                    "id": "faq_script",
                    "question": "Are you reading from a script?",
                    "answer": "Ha — I have notes in front of me, sure, but I'm not reading word for word. I just want to make sure I give you accurate information about what we offer. Is there something specific you'd like to know that I can answer more directly?",
                },
                {
                    "id": "faq_how_it_works",
                    "question": "How does it actually work?",
                    "answer": "When a customer calls your business number, instead of going to voicemail or ringing out, it connects to your Lynkflow agent. The agent greets them with your business name, asks what they need, collects their info, and if they want to book a job, it checks your calendar and locks in a time. After the call ends, you get a text or email with everything — who called, what they need, what was booked. You don't have to do anything differently.",
                },
                {
                    "id": "faq_sound_fake",
                    "question": "Will it sound fake? Will my customers know it's not a real person?",
                    "answer": "Honestly, modern AI voice systems sound very natural — most callers don't realize they're not talking to a human unless they're told. And even if a customer figures it out, the experience is still smooth and professional. They still get answered, their info is captured, and their job gets booked. That's what matters to them at the end of the day.",
                },
                {
                    "id": "faq_existing_number",
                    "question": "Do I have to change my phone number?",
                    "answer": "No — you keep your existing number. We either forward calls to the Lynkflow system or set it up to activate after a certain number of rings. Your number stays the same, nothing changes for your existing customers.",
                },
                {
                    "id": "faq_calendar",
                    "question": "How does the calendar booking work?",
                    "answer": "We connect it to your Google Calendar. The agent checks your availability in real time and only offers times that you're free. When a customer picks a time, it gets added to your calendar automatically. You'll get a notification immediately.",
                },
                {
                    "id": "faq_cancel",
                    "question": "Can I cancel anytime?",
                    "answer": "Yes — there's no long-term contract. After the setup, it's month to month. If it's not working for you, you can cancel with notice and we'll remove everything cleanly.",
                },
                {
                    "id": "faq_setup_time",
                    "question": "How long does it take to set up?",
                    "answer": "Usually 24 to 48 hours from the time we get your details. We handle all the technical setup on our end — you just need to give us some information about your business and how you want calls handled. After that, it's live.",
                },
                {
                    "id": "faq_what_if_fails",
                    "question": "What if the system goes down or makes a mistake?",
                    "answer": "Like any technology, there's no such thing as 100% uptime — but the systems we use are enterprise-grade and highly reliable. If there's ever an issue, we handle it. And if the agent ever can't handle a call, it can be set to fall back to your voicemail or a direct line so nothing is ever completely dropped.",
                },
                {
                    "id": "faq_multiple_jobs",
                    "question": "What if I have multiple employees or trucks?",
                    "answer": "No problem — the system works for the whole business, not just one person. It books based on whatever availability you set, so whether you have one truck or five, it only schedules what you can actually handle.",
                },
                {
                    "id": "faq_trial",
                    "question": "Is there a free trial?",
                    "answer": "We don't offer a free trial as a standard — the setup takes real time and resources to build specifically for your business. What I can do is walk you through a live demo of exactly how it works before you commit to anything. That way you see it in action first.",
                },
                {
                    "id": "faq_competitors",
                    "question": "How is this different from [other service] / I've heard of similar things",
                    "answer": "There are other answering services out there — most of them give you a generic recorded message or a call center with people who don't know your business. What's different about Lynkflow is that it's built specifically for your plumbing business, uses your name, handles your scheduling, and sends you a real summary after every call. It's not a generic box — it's a custom system.",
                },
                {
                    "id": "faq_payment",
                    "question": "How do I pay?",
                    "answer": "We accept card payments online — it's straightforward and secure. Once you're ready to move forward, I'll send you a link and we can get the setup started right away.",
                },
            ],
            "sample_label": None,
        },
        {
            "id": "difficult_calls",
            "title": "Difficult Calls",
            "icon": "⚠️",
            "explanation": "Not every call is clean. Some plumbers are rude, aggressive, or deliberately difficult. This section gives you the exact words for every hard situation. The rules: never match aggression with aggression, never apologize for calling if you've been professional, and never let someone speak to you in a way that crosses into personal abuse. Stay calm. Stay professional. Exit when needed.",
            "tips": [
                "Take a breath before responding to aggression — your tone is everything",
                "Never argue, never raise your voice, never take it personally",
                "You are allowed to end a call if someone is abusive — do it calmly",
                "Disposition the call accurately after — don't let a rough call affect your next one",
                "If you're shaken after a hard call, take 30 seconds before dialing again",
            ],
            "subsections": [
                {
                    "id": "diff_rude",
                    "label": "Rude / Dismissive",
                    "trigger": "Just hanging up / Being short / Clearly annoyed",
                    "script": "I completely understand — I'll be quick and then I'll let you go. One thing before I do: [deliver one-sentence hook]. If that's not relevant, I won't call again. Does that sound fair?",
                    "note": "Stay warm. Don't mirror their energy. Give them one strong hook and let them decide.",
                },
                {
                    "id": "diff_aggressive",
                    "label": "Aggressive / Yelling",
                    "trigger": "Raising their voice / Demanding you stop calling",
                    "script": "I hear you — I'm going to let you go right now. I apologize for catching you at a bad time. I'll make a note not to call again. Have a good day.",
                    "note": "Do not try to save this call. Disposition as NI or DNC based on what they said. Move on immediately.",
                },
                {
                    "id": "diff_cursing",
                    "label": "Cursing at You",
                    "trigger": "Using profanity directed at you",
                    "script": "I'm going to stop you right there — I'm happy to talk about this professionally, but I'm not going to continue the conversation if it goes this direction. If you'd like to hear what I have to say, I'm here. If not, no hard feelings — have a good day.",
                    "note": "Say this once, calmly. If they continue, hang up without another word. Disposition as HU.",
                },
                {
                    "id": "diff_racist",
                    "label": "Racist / Discriminatory Remarks",
                    "trigger": "Making comments about your accent, nationality, or ethnicity",
                    "script": "I appreciate you taking my call. I'm going to keep this professional and focus on why I called — [pivot back to pitch]. If at any point you'd prefer not to continue, that's completely fine too.",
                    "note": "You do not have to engage with the comment at all. Acknowledge nothing, pivot immediately. If it continues or escalates, end the call: 'I'm going to let you go — take care.' Hang up. Disposition HU.",
                },
                {
                    "id": "diff_messing",
                    "label": "Wasting Time / Messing Around",
                    "trigger": "Asking nonsense questions / Playing games / Clearly not serious",
                    "script": "I want to make sure I'm using your time well — are you the right person to talk to about this, or should I reach out another time?",
                    "note": "If they're clearly not serious after one redirect, disposition as HU and move on. Your time is worth protecting too.",
                },
                {
                    "id": "diff_repeat_caller",
                    "label": "They've Been Called Before",
                    "trigger": "I've already told you no / You keep calling / I asked you not to call",
                    "script": "I sincerely apologize for that — that should not have happened. I'm making a note right now to remove your number from our list. You won't receive another call from us. I'm sorry for the inconvenience.",
                    "note": "Disposition immediately as DNC. Do not try to pitch. Do not explain. Just apologize and remove.",
                },
                {
                    "id": "diff_recording",
                    "label": "They Say They're Recording",
                    "trigger": "I'm recording this call / This is being recorded",
                    "script": "That's completely fine — I have nothing to hide. My name is Odelyn, I'm calling from Lynkflow, and I'm reaching out to tell you about our call answering service for plumbing businesses. Happy to continue.",
                    "note": "Don't panic. Don't hang up. Just continue professionally. You're not doing anything wrong.",
                },
                {
                    "id": "diff_dnc",
                    "label": "They Demand to Be on DNC",
                    "trigger": "Put me on your do not call list / Remove my number",
                    "script": "Absolutely — I'm removing your number right now. You will not receive another call from us. I apologize for the interruption. Have a good day.",
                    "note": "Disposition as DNC immediately. No exceptions.",
                },
            ],
            "sample_label": "Hear how to handle it",
        },
        {
            "id": "close",
            "title": "The Close",
            "icon": "✅",
            "explanation": "You are the closer. There is no transfer. When the prospect warms up — stop pitching and start closing. The most common mistake is continuing to sell after the customer is already interested. Ask for the yes, then go quiet and let them answer. Silence is not awkward — it's pressure working in your favor.",
            "tips": [
                "The moment they stop objecting and start asking questions, that's your signal to close",
                "Ask, then be silent — do not fill the silence with more pitch",
                "If they hesitate, ask 'What's holding you back?' — not 'Are you sure?'",
                "After a yes, move immediately to next steps — don't linger",
                "Confirm everything clearly: price, timeline, what happens next",
            ],
            "subsections": [
                {
                    "id": "close_standard",
                    "label": "Standard Close",
                    "trigger": "They seem interested / Questions are slowing down",
                    "script": "Based on everything I've shared — does this sound like something that would help your business? ... [Silence. Wait for their answer.] ... [If yes]: Perfect. Here's what happens next — setup takes 24 to 48 hours, and we build the agent specifically around your business. The setup fee is [price]. We can take care of that right now and have you live by [day]. Does that work for you?",
                },
                {
                    "id": "close_soft",
                    "label": "Soft Close",
                    "trigger": "They're still a little unsure but warming up",
                    "script": "Let me ask you this — if you knew for a fact that this would save you at least two or three missed calls a month, would that be worth $100 a month to you? ... [Wait] ... Most owners say yes to that instantly. And that's really the only bet we're asking you to make.",
                },
                {
                    "id": "close_urgency",
                    "label": "Urgency Close",
                    "trigger": "They keep delaying but seem interested",
                    "script": "I totally understand wanting to think it through. Here's the thing though — every day without this, your phone is still ringing when you can't answer it. Those calls aren't waiting around. The sooner we get this set up, the sooner you stop losing them. Can we get the ball rolling today?",
                },
                {
                    "id": "close_post_yes",
                    "label": "After They Say Yes",
                    "trigger": "They've agreed to move forward",
                    "script": "Amazing — I'm glad we connected. So here's what's going to happen: I'll send you a short form to fill in your business details, then we start the build. Setup is 24 to 48 hours. I'll also send the payment link to the email you give me now. What's the best email address for you? ... [Get email] ... And your business name exactly as you want it to appear when the agent answers? ... [Confirm details] ... Perfect. You'll hear from us within the hour. Welcome aboard.",
                },
            ],
            "sample_label": "Hear the close",
        },
        {
            "id": "voicemail",
            "title": "Voicemail Script",
            "icon": "📱",
            "explanation": "When you hit voicemail, you have one shot to leave a message that makes them curious enough to call back — or at least recognize your name when you call again. Keep it under 20 seconds. Don't pitch on voicemail. Create just enough curiosity to stay in their head.",
            "tips": [
                "20 seconds maximum — nobody listens to long voicemails",
                "Speak clearly and slightly slower than normal",
                "Leave your number twice — once at the start and once at the end",
                "Don't pitch — create curiosity only",
                "Always call back anyway — don't wait for them to return your call",
            ],
            "script": "Hi, this is Odelyn calling from Lynkflow — I'm at [number]. I was calling to share something that's been helping plumbing businesses stop losing calls when they're out on jobs. I'll try you again, but feel free to call me back at [number]. Talk soon.",
            "sample_label": "Hear the voicemail",
        },
        {
            "id": "callback",
            "title": "Callback Confirmation",
            "icon": "📅",
            "explanation": "When someone says 'call me back later,' most agents nod and hang up — then call at a random time and start from zero again. You need to lock in a specific time and set expectations so the callback feels like a scheduled meeting, not a cold call all over again.",
            "tips": [
                "Always get a specific time — not 'sometime this week'",
                "Repeat the time back to them to confirm",
                "When you call back, reference the previous conversation immediately",
                "If they don't pick up at the agreed time, leave a voicemail referencing the scheduled callback",
            ],
            "script": "Completely understand — what's the best time to reach you? Is [morning / afternoon] better? ... [Confirm time] ... Perfect — so I'll call you back on [day] at [time]. And it's still [their number] that's best? ... Great. I'll have more details ready for you then. Talk soon. ... [On callback]: Hi [name], this is Odelyn from Lynkflow — we spoke [yesterday / earlier this week] and scheduled this call. Did I catch you at an okay time?",
            "sample_label": "Hear the callback script",
        },
        {
            "id": "dispositions",
            "title": "Call Dispositions",
            "icon": "📋",
            "explanation": "A disposition is how you tag a call after it ends. This is your call log — it tells you what happened, what to do next, and keeps your pipeline clean. Tag every single call. No exceptions. Accurate tagging is what separates a professional agent from a lazy one.",
            "tips": [
                "Tag the call within 30 seconds of hanging up — while it's fresh",
                "Be honest — wrong tagging messes up your own follow-up pipeline",
                "DNC is a legal obligation — never ignore it",
                "SALE should always be followed by an immediate confirmation email or message",
            ],
            "dispositions": [
                {"code": "SALE", "label": "Sale Closed", "description": "They said yes and committed to moving forward. Log name, business, price agreed, and email. Send confirmation immediately."},
                {"code": "CB", "label": "Callback Scheduled", "description": "They asked you to call back at a specific time. Log the exact time and any context they gave you."},
                {"code": "DMNI", "label": "Decision Maker Not In", "description": "You reached someone but the owner was unavailable. Log when to call back and who you spoke with."},
                {"code": "NI", "label": "Not Interested", "description": "They clearly declined after your full attempt. Three solid objections with no opening. Move on."},
                {"code": "ANSMACHINE", "label": "Answering Machine", "description": "Reached voicemail. Note whether you left a message or not. Always call back regardless."},
                {"code": "HU", "label": "Hung Up", "description": "They hung up during the call. Note at what point — greeting, pitch, objection. This tells you where the drop-off is."},
                {"code": "DNC", "label": "Do Not Call", "description": "They explicitly asked to be removed. Remove immediately and permanently. This is not optional."},
                {"code": "LB", "label": "Language Barrier", "description": "Communication wasn't possible. Log and move on — don't waste time on an unproductive call."},
                {"code": "BUSY", "label": "Busy / Rescheduled", "description": "They were busy but open. Always get a specific callback time before ending — 'try later' doesn't count."},
                {"code": "DEAD", "label": "Dead Air / No Response", "description": "Line connected but no one responded. Could be a bad connection or auto-dialer artifact. Log and move on."},
                {"code": "RING", "label": "No Answer / Ringing", "description": "Phone rang but no one picked up and no voicemail. Try again at a different time of day."},
                {"code": "WRONG", "label": "Wrong Number", "description": "Number doesn't belong to the business you were trying to reach. Update your lead sheet."},
            ],
            "sample_label": None,
        },
    ]
    return JSONResponse(modules)