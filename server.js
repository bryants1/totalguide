require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const { spawn } = require('child_process');  // ← ADD THIS
const fs = require('fs').promises;          // ← ADD THIS


const app = express();
const PORT = process.env.PORT || 3000;


// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Serve static files with proper MIME types for ES6 modules
app.use(express.static(path.join(__dirname, 'golf-course-admin'), {
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('.js')) {
            res.setHeader('Content-Type', 'application/javascript');
        }
        if (filePath.endsWith('.css')) {
            res.setHeader('Content-Type', 'text/css');
        }
    }
}));

// Initialize Supabase client
let supabaseClient = null;

// Helper function to initialize Supabase
function initializeSupabase(url, key) {
    try {
        supabaseClient = createClient(url, key);
        return { success: true };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// ADD THIS FUNCTION right after your initializeSupabase function (around line 40)

// Auto-connect to Supabase on server startup
async function connectServerToSupabase() {
    try {
        console.log('🔄 Attempting auto-connection to Supabase...');

        const supabaseUrl = process.env.SUPABASE_URL || 'https://pmpymmdayzqsxrbymxvh.supabase.co';
        const supabaseKey = process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y';

        if (supabaseUrl && supabaseKey) {
            const result = initializeSupabase(supabaseUrl, supabaseKey);
            if (result.success) {
                console.log('✅ Server connected to Supabase automatically on startup');

                // Test the connection
                try {
                    const { data, error } = await supabaseClient.from('initial_course_upload').select('count').limit(1);
                    if (error) {
                        console.log('⚠️ Supabase connected but query test failed:', error.message);
                    } else {
                        console.log('🔗 Supabase connection tested successfully');
                    }
                } catch (testError) {
                    console.log('⚠️ Supabase connection test failed:', testError.message);
                }

                return true;
            } else {
                console.log('❌ Auto-connection failed:', result.error);
            }
        } else {
            console.log('⚠️ Missing Supabase credentials for auto-connection');
        }
        return false;
    } catch (error) {
        console.log('❌ Server Supabase auto-connection failed:', error.message);
        return false;
    }
}

// Helper function to clean values
function cleanValue(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed === '' ? null : trimmed;
    }
    return value;
}

// Helper function to transform course scraping JSON structure to database format
// Fixed helper function to transform course scraping JSON structure to database format
// CORRECTED transformation function - matches exact database schema
function transformScrapingData(jsonData, courseNumber) {
    console.log(`🔧 Transforming data for course: ${courseNumber}`);

    const general = jsonData.general_info || {};
    const amenities = jsonData.amenities || general.amenities || {};
    const rates = jsonData.rates || general.rates || {};
    const history = jsonData.course_history || general.course_history || {};
    const awards = jsonData.awards || general.awards || {};
    const events = jsonData.amateur_professional_events || general.amateur_professional_events || {};
    const policies = jsonData.policies || general.policies || {};
    const social = jsonData.social || general.social || {};
    const sustainability = jsonData.sustainability || general.sustainability || {};
    const metadata = jsonData.metadata || general.metadata || {};

    // Log what we're extracting
    console.log(`📝 Extracting: name="${general.name?.value}", pricing_info_length=${rates.pricing_information?.value?.length || 0}`);

    const transformed = {
        course_number: courseNumber,

        // Basic Info - EXACT schema match
        name: general.name?.value || null,
        address: general.address?.value || null,
        phone: general.phone?.value || null,
        email: general.email?.value || null,
        website: general.website?.value || null,
        course_description: general.course_description?.value || [],
        scorecard_url: general.scorecard_url?.value || null,
        about_url: general.about_url?.value || null,
        membership_url: general.membership_url?.value || null,
        tee_time_url: general.tee_time_url?.value || null,
        course_type: general.course_type?.value || null,
        rates_url: general.rates_url?.value || null,

        // Course Types
        is_18_hole: general['18_hole_course']?.value || false,
        is_9_hole: general['9_hole_course']?.value || false,
        is_par_3_course: general.par_3_course?.value || false,
        is_executive_course: general.executive_course?.value || false,
        has_ocean_views: general.ocean_views?.value || false,
        has_scenic_views: general.scenic_views?.value || false,
        signature_holes: general.signature_holes?.value || [],

        // Pricing
        pricing_level: general.pricing_level?.value || null,
        pricing_level_description: general.pricing_level?.description || null,
        typical_18_hole_rate: general.pricing_level?.typical_18_hole_rate || null,
        pricing_information: rates.pricing_information?.value || null,

        // Amenities
        has_pro_shop: amenities.pro_shop?.available || false,
        pro_shop_details: amenities.pro_shop?.value || [],
        has_driving_range: amenities.driving_range?.available || false,
        driving_range_details: amenities.driving_range?.value || [],
        has_practice_green: amenities.practice_green?.available || false,
        practice_green_details: amenities.practice_green?.value || [],
        has_short_game_area: amenities.short_game_practice_area?.available || false,
        short_game_area_details: amenities.short_game_practice_area?.value || [],
        has_clubhouse: amenities.clubhouse?.available || false,
        clubhouse_details: amenities.clubhouse?.value || [],
        has_locker_rooms: amenities.locker_rooms?.available || false,
        locker_room_details: amenities.locker_rooms?.value || [],
        has_showers: amenities.showers?.available || false,
        shower_details: amenities.showers?.value || [],
        food_beverage_options: amenities.food_beverage_options?.value || null,
        food_beverage_description: amenities.food_beverage_options_description?.value || null,
        has_beverage_cart: amenities.beverage_cart?.available || false,
        beverage_cart_details: amenities.beverage_cart?.value || [],
        has_banquet_facilities: amenities.banquet_facilities?.available || false,
        banquet_facilities_details: amenities.banquet_facilities?.value || [],

        // Course History
        course_history_general: history.general?.value || [],
        architect: history.architect?.value || null,
        year_built: history.year_built?.value || null,
        notable_events: history.notable_events?.value || [],
        design_features: history.design_features?.value || [],

        // Awards
        recognitions: awards.recognitions?.value || [],
        rankings: awards.rankings?.value || [],
        certifications: awards.certifications?.value || [],

        // Events
        amateur_tournaments: events.amateur_tournaments?.value || [],
        professional_tournaments: events.professional_tournaments?.value || [],
        charity_events: events.charity_events?.value || [],
        event_contact: events.contact_for_events?.value || null,

        // Policies
        course_policies: policies.course_policies?.value || null,

        // Social Media
        facebook_url: social.facebook_url?.value || null,
        instagram_url: social.instagram_url?.value || null,
        twitter_url: social.twitter_url?.value || null,
        youtube_url: social.youtube_url?.value || null,
        tiktok_url: social.tiktok_url?.value || null,

        // Sustainability
        sustainability_general: sustainability.general?.value || [],
        sustainability_certifications: sustainability.certifications?.value || [],
        sustainability_practices: sustainability.practices?.value || [],

        // Metadata
        pages_crawled: metadata.pages_crawled?.value || 0,
        ml_extractions: metadata.ml_extractions?.value || 0,
        regex_extractions: metadata.regex_extractions?.value || 0,
        last_updated: metadata.last_updated?.value || new Date().toISOString(),
        spider_version: metadata.spider_version?.value || null,

        // Import metadata
        import_date: new Date().toISOString(),
        data_source: 'course_scraping'
    };

    // Log what we transformed
    const nonNullFields = Object.entries(transformed).filter(([key, value]) => {
        if (value === null || value === undefined) return false;
        if (Array.isArray(value) && value.length === 0) return false;
        if (typeof value === 'string' && value.trim() === '') return false;
        return true;
    });

    console.log(`✅ Transformed ${nonNullFields.length} non-null fields for ${courseNumber}`);
    console.log(`📊 Key fields: name=${!!transformed.name}, pricing=${!!transformed.pricing_information}, phone=${!!transformed.phone}`);

    return transformed;
}
// Middleware to check if Supabase is initialized
function requireSupabase(req, res, next) {
    if (!supabaseClient) {
        return res.status(400).json({
            success: false,
            error: 'Supabase client not initialized. Please call /api/connect first.'
        });
    }
    next();
}

// Connect to Supabase
// Serve index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'golf-course-admin', 'index.html'));
});
app.post('/api/connect', (req, res) => {
    const { supabaseUrl, supabaseKey } = req.body;

    if (!supabaseUrl || !supabaseKey) {
        return res.status(400).json({
            success: false,
            error: 'supabaseUrl and supabaseKey are required'
        });
    }

    const result = initializeSupabase(supabaseUrl, supabaseKey);

    if (result.success) {
        res.json({
            success: true,
            message: 'Connected to Supabase successfully'
        });
    } else {
        res.status(500).json({
            success: false,
            error: result.error
        });
    }
});

// Import Course Scores
app.post('/api/import/scores', requireSupabase, async (req, res) => {
    try {
        const { data: scoresArray } = req.body;

        if (!Array.isArray(scoresArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of score objects'
            });
        }

        console.log(`Processing ${scoresArray.length} course scores...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const scoreData of scoresArray) {
            try {
                const courseNumber = scoreData.course_id || scoreData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: 'Unknown',
                        error: 'No course number/ID found'
                    });
                    continue;
                }

                // Calculate category totals
                const scores = scoreData.scores || {};
                const courseConditions = scores['Course Conditions'] || {};
                const courseLayout = scores['Course Layout & Design'] || {};
                const amenities = scores['Amenities'] || {};
                const playerExperience = scores['Player Experience'] || {};

                const courseConditionsScore = Object.values(courseConditions).reduce((sum, val) => sum + (parseInt(val) || 0), 0);
                const courseLayoutScore = Object.values(courseLayout).reduce((sum, val) => sum + (parseInt(val) || 0), 0);
                const amenitiesScore = Object.values(amenities).reduce((sum, val) => sum + (parseInt(val) || 0), 0);
                const playerExperienceScore = Object.values(playerExperience).reduce((sum, val) => sum + (parseInt(val) || 0), 0);

                const courseScoreRecord = {
                    course_number: courseNumber,
                    course_name: cleanValue(scoreData.course_name),
                    total_score: scoreData.total_score || 0,
                    max_score: scoreData.max_score || 100,
                    percentage: scoreData.percentage || 0,

                    course_conditions_score: courseConditionsScore,
                    course_conditions_max: 28,
                    course_layout_design_score: courseLayoutScore,
                    course_layout_design_max: 25,
                    amenities_score: amenitiesScore,
                    amenities_max: 22,
                    player_experience_score: playerExperienceScore,
                    player_experience_max: 25,

                    scores: JSON.stringify(scoreData.scores || {}),
                    detailed_scores_with_explanations: JSON.stringify(scoreData.detailed_scores_with_explanations || {}),

                    scoring_date: new Date().toISOString(),
                    data_source: 'api_import',
                    scoring_version: '1.0'
                };

                // Try upsert first, fall back to delete+insert if needed
                let { error } = await supabaseClient
                    .from('course_scores')
                    .upsert(courseScoreRecord);

                if (error && error.message.includes('unique or exclusion constraint')) {
                    await supabaseClient
                        .from('course_scores')
                        .delete()
                        .eq('course_number', courseNumber);

                    const { error: insertError } = await supabaseClient
                        .from('course_scores')
                        .insert(courseScoreRecord);

                    error = insertError;
                }

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Course Scores insert error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: scoreData.course_id || scoreData.course_number || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing course score:`, error);
            }
        }

        res.json({
            success: true,
            message: `Course scores import completed`,
            stats: {
                total: scoresArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Course scores import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Vector Attributes
app.post('/api/import/vector-attributes', requireSupabase, async (req, res) => {
    try {
        const { data: vectorArray } = req.body;

        if (!Array.isArray(vectorArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of vector attribute objects'
            });
        }

        console.log(`Processing ${vectorArray.length} vector attribute records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const vectorData of vectorArray) {
            try {
                // Extract course number from course_path or use direct course_number
                const pathMatch = vectorData.course_path?.match(/([A-Z]{2}-[\d-]+)/);
                const courseNumber = pathMatch ? pathMatch[1] : vectorData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: vectorData.course_path || 'Unknown',
                        error: 'Could not extract course number'
                    });
                    continue;
                }

                const attributes = vectorData.vector_attributes || {};
                const compositeScores = attributes.composite_scores || {};

                const vectorRecord = {
                    course_number: courseNumber,
                    course_path: cleanValue(vectorData.course_path),
                    generation_timestamp: vectorData.generation_timestamp ? new Date(vectorData.generation_timestamp).toISOString() : null,

                    // Analysis config
                    max_satellite_images: vectorData.analysis_config?.max_satellite_images,
                    max_elevation_images: vectorData.analysis_config?.max_elevation_images,
                    analyze_individual_holes: vectorData.analysis_config?.analyze_individual_holes,

                    // Core attributes
                    ball_findability_score: cleanValue(attributes.ball_findability_score),
                    tree_coverage_density: cleanValue(attributes.tree_coverage_density),
                    visual_tightness: cleanValue(attributes.visual_tightness),
                    natural_integration: cleanValue(attributes.natural_integration),
                    water_prominence: cleanValue(attributes.water_prominence),
                    course_openness: cleanValue(attributes.course_openness),
                    terrain_visual_complexity: cleanValue(attributes.terrain_visual_complexity),
                    elevation_feature_prominence: cleanValue(attributes.elevation_feature_prominence),

                    // Categorical attributes
                    design_style_category: cleanValue(attributes.design_style_category),
                    routing_style: cleanValue(attributes.routing_style),
                    analysis_method: cleanValue(attributes.analysis_method),

                    // Composite scores
                    beginner_friendly_score: cleanValue(compositeScores.beginner_friendly_score),
                    ball_loss_risk_score: cleanValue(compositeScores.ball_loss_risk_score),
                    open_course_feeling: cleanValue(compositeScores.open_course_feeling),
                    natural_character_score: cleanValue(compositeScores.natural_character_score),
                    water_course_score: cleanValue(compositeScores.water_course_score),

                    // Analysis counts
                    satellite_analysis_count: cleanValue(vectorData.satellite_analysis_count),
                    elevation_analysis_count: cleanValue(vectorData.elevation_analysis_count),

                    // Full JSON storage
                    vector_attributes: JSON.stringify(vectorData.vector_attributes || {}),
                    attribute_schemas: JSON.stringify(vectorData.attribute_schemas || {}),
                    analysis_config: JSON.stringify(vectorData.analysis_config || {})
                };

                // Try upsert first, fall back to delete+insert if needed
                let { error } = await supabaseClient
                    .from('course_vector_attributes')
                    .upsert(vectorRecord);

                if (error && error.message.includes('unique or exclusion constraint')) {
                    await supabaseClient
                        .from('course_vector_attributes')
                        .delete()
                        .eq('course_number', courseNumber);

                    const { error: insertError } = await supabaseClient
                        .from('course_vector_attributes')
                        .insert(vectorRecord);

                    error = insertError;
                }

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Vector Attributes insert error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: vectorData.course_path || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing vector attributes:`, error);
            }
        }

        res.json({
            success: true,
            message: `Vector attributes import completed`,
            stats: {
                total: vectorArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Vector attributes import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Comprehensive Analysis
app.post('/api/import/comprehensive-analysis', requireSupabase, async (req, res) => {
    try {
        const { data: analysisArray } = req.body;

        if (!Array.isArray(analysisArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of comprehensive analysis objects'
            });
        }

        console.log(`Processing ${analysisArray.length} comprehensive analysis records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const analysisData of analysisArray) {
            try {
                // Extract course number from course_name or use direct course_number
                const nameMatch = analysisData.course_name?.match(/([A-Z]{2}-[\d-]+)/);
                const courseNumber = nameMatch ? nameMatch[1] : analysisData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: analysisData.course_name || 'Unknown',
                        error: 'Could not extract course number'
                    });
                    continue;
                }

                const strategic = analysisData.strategic_analysis || {};
                const strategySummary = strategic.course_strategy_summary || {};
                const landingZones = strategySummary.landing_zone_analysis || {};
                const bunkerPos = strategySummary.bunker_positioning || {};
                const doglegAnalysis = strategySummary.dogleg_analysis || {};
                const handedness = strategySummary.handedness_advantage || {};

                const weather = analysisData.weather_analysis || {};
                const elevation = analysisData.elevation_analysis || {};
                const elevationSummary = elevation.course_elevation_summary || {};

                const comprehensiveRecord = {
                    course_number: courseNumber,
                    course_name: cleanValue(analysisData.course_name),
                    analysis_timestamp: analysisData.analysis_timestamp ? new Date(analysisData.analysis_timestamp).toISOString() : null,

                    // Location
                    latitude: cleanValue(analysisData.coordinates?.latitude),
                    longitude: cleanValue(analysisData.coordinates?.longitude),
                    total_holes: cleanValue(analysisData.total_holes),

                    // Strategic summary
                    left_biased_holes: cleanValue(bunkerPos.left_biased_holes),
                    right_biased_holes: cleanValue(bunkerPos.right_biased_holes),
                    course_bunker_bias: cleanValue(bunkerPos.course_bunker_bias),
                    left_doglegs: cleanValue(doglegAnalysis.left_doglegs),
                    right_doglegs: cleanValue(doglegAnalysis.right_doglegs),
                    total_doglegs: cleanValue(doglegAnalysis.total_doglegs),
                    dogleg_balance: cleanValue(doglegAnalysis.dogleg_balance),
                    overall_handedness_advantage: cleanValue(handedness.overall_advantage),

                    // Landing zone safety
                    short_hitter_safety_pct: cleanValue(landingZones.short_hitter?.safety_percentage),
                    average_hitter_safety_pct: cleanValue(landingZones.average_hitter?.safety_percentage),
                    long_hitter_safety_pct: cleanValue(landingZones.long_hitter?.safety_percentage),

                    // Weather summary
                    weather_data_source: cleanValue(weather.weather_data_source),
                    weather_years_analyzed: cleanValue(weather.weather_years_analyzed),
                    total_days_analyzed: cleanValue(weather.total_days_analyzed),
                    avg_temp_c: cleanValue(weather.avg_temp_C),
                    golf_season_avg_temp_c: cleanValue(weather.golf_season_avg_temp_C),
                    golf_season_length_months: cleanValue(weather.golf_season_length_months),
                    golf_season_type: cleanValue(weather.golf_season_type),
                    rainy_days_pct: cleanValue(weather.rainy_days_pct),
                    heavy_rain_days_pct: cleanValue(weather.heavy_rain_days_pct),
                    avg_wind_kph: cleanValue(weather.avg_wind_kph),
                    windy_days_pct: cleanValue(weather.windy_days_pct),
                    best_golf_month: cleanValue(weather.best_golf_month),
                    best_golf_score: cleanValue(weather.best_golf_score),
                    worst_golf_month: cleanValue(weather.worst_golf_month),
                    worst_golf_score: cleanValue(weather.worst_golf_score),

                    // Elevation summary
                    total_elevation_change_m: cleanValue(elevationSummary.total_elevation_change_m),
                    average_elevation_change_m: cleanValue(elevationSummary.average_elevation_change_m),
                    max_single_hole_change_m: cleanValue(elevationSummary.max_single_hole_change_m),
                    uphill_holes: cleanValue(elevationSummary.uphill_holes),
                    downhill_holes: cleanValue(elevationSummary.downhill_holes),
                    flat_holes: cleanValue(elevationSummary.flat_holes),
                    extreme_difficulty_holes: cleanValue(elevationSummary.extreme_difficulty_holes),
                    challenging_difficulty_holes: cleanValue(elevationSummary.challenging_difficulty_holes),
                    most_challenging_hole: cleanValue(elevationSummary.most_challenging_hole),

                    // Full JSON storage
                    strategic_analysis: JSON.stringify(analysisData.strategic_analysis || {}),
                    weather_analysis: JSON.stringify(analysisData.weather_analysis || {}),
                    elevation_analysis: JSON.stringify(analysisData.elevation_analysis || {})
                };

                // Try upsert first, fall back to delete+insert if needed
                let { error } = await supabaseClient
                    .from('course_comprehensive_analysis')
                    .upsert(comprehensiveRecord);

                if (error && error.message.includes('unique or exclusion constraint')) {
                    await supabaseClient
                        .from('course_comprehensive_analysis')
                        .delete()
                        .eq('course_number', courseNumber);

                    const { error: insertError } = await supabaseClient
                        .from('course_comprehensive_analysis')
                        .insert(comprehensiveRecord);

                    error = insertError;
                }

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Comprehensive Analysis insert error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: analysisData.course_name || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing comprehensive analysis:`, error);
            }
        }

        res.json({
            success: true,
            message: `Comprehensive analysis import completed`,
            stats: {
                total: analysisArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Comprehensive analysis import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Review Data
app.post('/api/import/reviews', requireSupabase, async (req, res) => {
    try {
        const { data: reviewsArray } = req.body;

        if (!Array.isArray(reviewsArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of review objects'
            });
        }

        console.log(`Processing ${reviewsArray.length} review summary records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const reviewData of reviewsArray) {
            try {
                // Extract course number from course_prefix
                const courseNumber = reviewData.course_prefix;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: reviewData.course_name || 'Unknown',
                        error: 'No course_prefix found'
                    });
                    continue;
                }

                const formAverages = reviewData.form_category_averages || {};
                const textInsights = reviewData.text_insight_averages || {};
                const reviewsBySource = reviewData.reviews_by_source || {};

                const reviewRecord = {
                    course_number: courseNumber,
                    course_name: cleanValue(reviewData.course_name),

                    // Overall stats
                    total_reviews: reviewData.total_reviews || 0,
                    overall_rating: reviewData.overall_rating || null,
                    recommend_percent: reviewData.recommend_percent || null,

                    // Reviews by source
                    golfnow_reviews: reviewsBySource.GolfNow || 0,
                    golfpass_reviews: reviewsBySource.GolfPass || 0,

                    // Form category averages
                    conditions_rating: cleanValue(formAverages.Conditions),
                    value_rating: cleanValue(formAverages.Value),
                    friendliness_rating: cleanValue(formAverages.Friendliness),
                    pace_rating: cleanValue(formAverages.Pace),
                    amenities_rating: cleanValue(formAverages.Amenities),
                    difficulty_rating: cleanValue(formAverages.Difficulty),
                    layout_rating: cleanValue(formAverages.Layout),

                    // Key text insights
                    fairways_score: cleanValue(textInsights.Fairways),
                    greens_score: cleanValue(textInsights.Greens),
                    bunkers_score: cleanValue(textInsights.Bunkers),
                    tee_boxes_score: cleanValue(textInsights['Tee Boxes']),
                    shot_variety_score: cleanValue(textInsights['Shot Variety / Hole Uniqueness']),
                    signature_holes_score: cleanValue(textInsights['Signature Holes / Quirky/Fun Design Features']),
                    water_ob_placement_score: cleanValue(textInsights['Water & OB Placement']),
                    overall_feel_scenery_score: cleanValue(textInsights['Overall feel / Scenery']),
                    green_complexity_score: cleanValue(textInsights['Green Complexity']),
                    staff_friendliness_score: cleanValue(textInsights['Staff Friendliness, After-Round Experience']),
                    green_fees_quality_score: cleanValue(textInsights['Green Fees vs. Quality']),
                    replay_value_score: cleanValue(textInsights['Replay Value']),
                    ease_walking_score: cleanValue(textInsights['Ease of Walking']),
                    pace_play_score: cleanValue(textInsights['Pace of Play']),
                    availability_score: cleanValue(textInsights.Availability),

                    // Store full JSON data
                    form_category_averages: JSON.stringify(formAverages),
                    text_insight_averages: JSON.stringify(textInsights),
                    top_text_themes: JSON.stringify(reviewData.top_text_themes || []),
                    reviews_by_source: JSON.stringify(reviewsBySource),

                    // Metadata
                    import_date: new Date().toISOString(),
                    data_source: 'api_import'
                };

                // Try upsert first, fall back to delete+insert if needed
                let { error } = await supabaseClient
                    .from('course_reviews')
                    .upsert(reviewRecord);

                if (error && error.message.includes('unique or exclusion constraint')) {
                    await supabaseClient
                        .from('course_reviews')
                        .delete()
                        .eq('course_number', courseNumber);

                    const { error: insertError } = await supabaseClient
                        .from('course_reviews')
                        .insert(reviewRecord);

                    error = insertError;
                }

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Review import error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: reviewData.course_prefix || reviewData.course_name || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing review data:`, error);
            }
        }

        res.json({
            success: true,
            message: `Review data import completed`,
            stats: {
                total: reviewsArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Review import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Course Scraping Data - Single course
app.post('/api/import/course-scraping/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;
        const scrapingData = req.body;

        // Validate course number exists
        const { data: courseExists, error: courseError } = await supabaseClient
            .from('initial_course_upload')
            .select('course_number')
            .eq('course_number', courseNumber)
            .single();

        if (courseError || !courseExists) {
            return res.status(404).json({
                success: false,
                error: 'Course not found',
                message: `Course number ${courseNumber} does not exist in the database`
            });
        }

        // Transform the JSON data to match our database schema
        const transformedData = transformScrapingData(scrapingData, courseNumber);

        // Try upsert first, fall back to delete+insert if needed
        let { error } = await supabaseClient
            .from('course_scraping_data')
            .upsert(transformedData, {
                onConflict: 'course_number'
            });

        if (error && error.message.includes('unique or exclusion constraint')) {
            await supabaseClient
                .from('course_scraping_data')
                .delete()
                .eq('course_number', courseNumber);

            const { error: insertError } = await supabaseClient
                .from('course_scraping_data')
                .insert(transformedData);

            error = insertError;
        }

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            message: `Course scraping data imported successfully for course ${courseNumber}`,
            courseNumber: courseNumber
        });

    } catch (error) {
        console.error('Import error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Import Course Scraping Data - Bulk import
app.post('/api/import/course-scraping', requireSupabase, async (req, res) => {
    try {
        const { data: scrapingArray } = req.body;

        if (!Array.isArray(scrapingArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of course scraping objects'
            });
        }

        console.log(`Processing ${scrapingArray.length} course scraping records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const scrapingData of scrapingArray) {
            try {
                // Extract course number from filename or other identifier
                const courseNumber = scrapingData.course_number ||
                                   scrapingData.general_info?.course_id?.value ||
                                   scrapingData.metadata?.course_number?.value;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: scrapingData.general_info?.name?.value || 'Unknown',
                        error: 'Could not extract course number'
                    });
                    continue;
                }

                // Transform the JSON data to match our database schema
                const transformedData = transformScrapingData(scrapingData, courseNumber);

                // Try upsert first, fall back to delete+insert if needed
                let { error } = await supabaseClient
                    .from('course_scraping_data')
                    .upsert(transformedData, {
                        onConflict: 'course_number'
                    });

                if (error && error.message.includes('unique or exclusion constraint')) {
                    await supabaseClient
                        .from('course_scraping_data')
                        .delete()
                        .eq('course_number', courseNumber);

                    const { error: insertError } = await supabaseClient
                        .from('course_scraping_data')
                        .insert(transformedData);

                    error = insertError;
                }

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Course scraping import error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: scrapingData.general_info?.name?.value || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing course scraping data:`, error);
            }
        }

        res.json({
            success: true,
            message: `Course scraping data import completed`,
            stats: {
                total: scrapingArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Course scraping import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Pars Data
app.post('/api/import/pars', requireSupabase, async (req, res) => {
    try {
        const { data: parsArray } = req.body;

        if (!Array.isArray(parsArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of pars objects'
            });
        }

        console.log(`Processing ${parsArray.length} pars records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const parsData of parsArray) {
            try {
                const courseNumber = parsData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: parsData.course_name || 'Unknown',
                        error: 'No course_number found'
                    });
                    continue;
                }

                // Verify course exists in initial_course_upload
                const { data: courseExists, error: courseError } = await supabaseClient
                    .from('initial_course_upload')
                    .select('course_number')
                    .eq('course_number', courseNumber)
                    .single();

                if (courseError || !courseExists) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: `Course not found in initial_course_upload: ${courseNumber}`
                    });
                    continue;
                }

                const parsRecord = {
                    course_name: cleanValue(parsData.course_name),
                    course_number: courseNumber,
                    data_group: parsData.data_group || 'initial_upload_data', // ← Use incoming value or default
                    hole_1: cleanValue(parsData.hole_1),
                    hole_2: cleanValue(parsData.hole_2),
                    hole_3: cleanValue(parsData.hole_3),
                    hole_4: cleanValue(parsData.hole_4),
                    hole_5: cleanValue(parsData.hole_5),
                    hole_6: cleanValue(parsData.hole_6),
                    hole_7: cleanValue(parsData.hole_7),
                    hole_8: cleanValue(parsData.hole_8),
                    hole_9: cleanValue(parsData.hole_9),
                    out_9: cleanValue(parsData.out_9),
                    hole_10: cleanValue(parsData.hole_10),
                    hole_11: cleanValue(parsData.hole_11),
                    hole_12: cleanValue(parsData.hole_12),
                    hole_13: cleanValue(parsData.hole_13),
                    hole_14: cleanValue(parsData.hole_14),
                    hole_15: cleanValue(parsData.hole_15),
                    hole_16: cleanValue(parsData.hole_16),
                    hole_17: cleanValue(parsData.hole_17),
                    hole_18: cleanValue(parsData.hole_18),
                    in_9: cleanValue(parsData.in_9),
                    total_par: cleanValue(parsData.total_par),
                    verified: parsData.verified !== undefined ? parsData.verified : false
                };

                // Insert into course_pars table
                const { error } = await supabaseClient
                    .from('course_pars')
                    .upsert(parsRecord, {
                        onConflict: 'course_name,course_number,data_group'
                    });

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Pars insert error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: parsData.course_number || parsData.course_name || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing pars data:`, error);
            }
        }

        res.json({
            success: true,
            message: `Pars import completed`,
            stats: {
                total: parsArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Pars import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Import Tees Data
app.post('/api/import/tees', requireSupabase, async (req, res) => {
    try {
        const { data: teesArray } = req.body;

        if (!Array.isArray(teesArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of tees objects'
            });
        }

        console.log(`Processing ${teesArray.length} tees records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const teesData of teesArray) {
            try {
                const courseNumber = teesData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: teesData.course_name || 'Unknown',
                        error: 'No course_number found'
                    });
                    continue;
                }

                // Verify course exists in initial_course_upload
                const { data: courseExists, error: courseError } = await supabaseClient
                    .from('initial_course_upload')
                    .select('course_number')
                    .eq('course_number', courseNumber)
                    .single();

                if (courseError || !courseExists) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: `Course not found in initial_course_upload: ${courseNumber}`
                    });
                    continue;
                }

                const teesRecord = {
                    course_name: cleanValue(teesData.course_name),
                    course_number: courseNumber,
                    data_group: teesData.data_group || 'initial_upload_data', // ← Use incoming value or default
                    tee_number: cleanValue(teesData.tee_number),
                    tee_name: cleanValue(teesData.tee_name),
                    total_yardage: cleanValue(teesData.total_yardage),
                    rating: cleanValue(teesData.rating),
                    slope: cleanValue(teesData.slope),
                    hole_1: cleanValue(teesData.hole_1),
                    hole_2: cleanValue(teesData.hole_2),
                    hole_3: cleanValue(teesData.hole_3),
                    hole_4: cleanValue(teesData.hole_4),
                    hole_5: cleanValue(teesData.hole_5),
                    hole_6: cleanValue(teesData.hole_6),
                    hole_7: cleanValue(teesData.hole_7),
                    hole_8: cleanValue(teesData.hole_8),
                    hole_9: cleanValue(teesData.hole_9),
                    out_9: cleanValue(teesData.out_9),
                    hole_10: cleanValue(teesData.hole_10),
                    hole_11: cleanValue(teesData.hole_11),
                    hole_12: cleanValue(teesData.hole_12),
                    hole_13: cleanValue(teesData.hole_13),
                    hole_14: cleanValue(teesData.hole_14),
                    hole_15: cleanValue(teesData.hole_15),
                    hole_16: cleanValue(teesData.hole_16),
                    hole_17: cleanValue(teesData.hole_17),
                    hole_18: cleanValue(teesData.hole_18),
                    in_9: cleanValue(teesData.in_9),
                    scrape_source: teesData.scrape_source || null,
                    verified: teesData.verified !== undefined ? teesData.verified : false
                };

                // Insert into course_tees table
                const { error } = await supabaseClient
                    .from('course_tees')
                    .upsert(teesRecord, {
                        onConflict: 'course_name,course_number,tee_name,data_group'
                    });

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Tees insert error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: teesData.course_number || teesData.course_name || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing tees data:`, error);
            }
        }

        res.json({
            success: true,
            message: `Tees import completed`,
            stats: {
                total: teesArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Tees import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get pars data for a specific course
app.get('/api/pars/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;
        const { data_group = 'initial_upload_data' } = req.query;

        const { data, error } = await supabaseClient
            .from('course_pars')
            .select('*')
            .eq('course_number', courseNumber)
            .eq('data_group', data_group)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: 'Not found',
                message: `No pars data found for course ${courseNumber} in ${data_group}`
            });
        }

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            data: data
        });

    } catch (error) {
        console.error('Retrieval error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get tees data for a specific course
app.get('/api/tees/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;
        const { data_group = 'initial_upload_data' } = req.query;

        const { data, error } = await supabaseClient
            .from('course_tees')
            .select('*')
            .eq('course_number', courseNumber)
            .eq('data_group', data_group)
            .order('tee_number');

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            data: data || []
        });

    } catch (error) {
        console.error('Retrieval error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Promote pars and tees from initial_upload_data to primary_data for a specific course
app.post('/api/promote-to-primary/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        console.log(`Promoting course ${courseNumber} to primary data...`);

        // Call the PostgreSQL function to promote the data
        const { data, error } = await supabaseClient
            .rpc('promoteinitialtoprimary', {
                p_course_name: null, // We'll use course_number matching instead
                p_course_number: courseNumber
            });

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        // Verify the promotion worked by checking for primary data
        const { data: parsData } = await supabaseClient
            .from('course_pars')
            .select('id')
            .eq('course_number', courseNumber)
            .eq('data_group', 'primary_data');

        const { data: teesData } = await supabaseClient
            .from('course_tees')
            .select('id')
            .eq('course_number', courseNumber)
            .eq('data_group', 'primary_data');

        res.status(200).json({
            success: true,
            message: `Successfully promoted course ${courseNumber} to primary data`,
            promoted: {
                pars: parsData?.length || 0,
                tees: teesData?.length || 0
            }
        });

    } catch (error) {
        console.error('Promotion error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Import/Upload Initial Course Data - Single Course Upload with Upsert
app.post('/api/initial-course-upload', requireSupabase, async (req, res) => {
    try {
        const courseData = req.body;

        if (!courseData.course_number) {
            return res.status(400).json({
                success: false,
                error: 'course_number is required'
            });
        }

        console.log(`📝 Uploading/updating course: ${courseData.course_number}`);

        // Clean all values before inserting
        const cleanedData = {};
        for (const [key, value] of Object.entries(courseData)) {
            cleanedData[key] = cleanValue(value);
        }

        // Set timestamps
        cleanedData.updated_at = new Date().toISOString();
        if (!cleanedData.created_at) {
            cleanedData.created_at = new Date().toISOString();
        }

        // Upsert the course data (update if exists, insert if not)
        const { data, error } = await supabaseClient
            .from('initial_course_upload')
            .upsert(cleanedData, {
                onConflict: 'course_number',
                ignoreDuplicates: false
            })
            .select('course_number');

        if (error) {
            console.error(`Database error for course ${courseData.course_number}:`, error);
            return res.status(400).json({
                success: false,
                error: 'Database error',
                message: error.message,
                course_number: courseData.course_number
            });
        }

        res.status(201).json({
            success: true,
            action: 'upserted',
            course_number: courseData.course_number,
            message: `Course ${courseData.course_number} successfully uploaded`
        });

    } catch (error) {
        console.error('Initial course upload error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get summary of pars and tees data by data_group
app.get('/api/pars-tees-summary', requireSupabase, async (req, res) => {
    try {
        // Get pars summary
        const { data: parsSummary, error: parsError } = await supabaseClient
            .from('course_pars')
            .select('data_group')
            .then(result => {
                if (result.error) return result;

                const summary = result.data.reduce((acc, row) => {
                    acc[row.data_group] = (acc[row.data_group] || 0) + 1;
                    return acc;
                }, {});

                return { data: summary, error: null };
            });

        // Get tees summary
        const { data: teesSummary, error: teesError } = await supabaseClient
            .from('course_tees')
            .select('data_group')
            .then(result => {
                if (result.error) return result;

                const summary = result.data.reduce((acc, row) => {
                    acc[row.data_group] = (acc[row.data_group] || 0) + 1;
                    return acc;
                }, {});

                return { data: summary, error: null };
            });

        if (parsError || teesError) {
            console.error('Database error:', parsError || teesError);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: (parsError || teesError).message
            });
        }

        res.status(200).json({
            success: true,
            summary: {
                pars: parsSummary || {},
                tees: teesSummary || {}
            }
        });

    } catch (error) {
        console.error('Summary error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get course scraping data
app.get('/api/course-scraping/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        const { data, error } = await supabaseClient
            .from('course_scraping_data')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: 'Not found',
                message: `No scraping data found for course ${courseNumber}`
            });
        }

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            data: data
        });

    } catch (error) {
        console.error('Retrieval error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// ADD this endpoint to your server.js file

// Run website scraper for a specific course
// Run website scraper for a specific course
// REPLACE this section in your server.js file (around line 700-800)
// Run website scraper for a specific course - FIXED VERSION
app.post('/api/run-website-scraper', requireSupabase, async (req, res) => {
    try {
        const { course_number, website_url, force_reprocess = false, max_pages = 10 } = req.body;

        if (!course_number || !website_url) {
            return res.status(400).json({
                success: false,
                error: 'course_number and website_url are required'
            });
        }

        console.log(`🌐 API: Starting website scraper for ${course_number}`);
        console.log(`🔗 URL: ${website_url}`);
        console.log(`🔥 Force reprocess: ${force_reprocess}`);


        // Path to your updated runner script - ADJUST THIS PATH AS NEEDED
        const runnerPath = path.join(__dirname, '../../pythonScripts/Projects/new_course_scraper/database_integrated_runner.py');

        // Verify the runner script exists
        try {
            await fs.access(runnerPath);
            console.log(`✅ Found runner script at: ${runnerPath}`);
        } catch (error) {
            console.error(`❌ Runner script not found at: ${runnerPath}`);
            return res.status(500).json({
                success: false,
                error: 'Runner script not found',
                message: `Please check the path: ${runnerPath}`,
                course_number
            });
        }

        // Build command arguments
        const args = [
            runnerPath,
            '--course', course_number,
            '--output', 'scraped_courses',
            '--api-url', 'http://localhost:3000'
        ];

        // Add force flag if needed - THIS IS THE KEY PART!
        if (force_reprocess) {
            args.push('--force');
            console.log(`🔥 Added --force flag to command`);
        }

        console.log(`🚀 Executing: python ${args.join(' ')}`);

        // Execute the runner script
        const pythonProcess = spawn('python', args, {
            stdio: ['pipe', 'pipe', 'pipe'],
            cwd: process.cwd()
        });

        let stdout = '';
        let stderr = '';

        pythonProcess.stdout.on('data', (data) => {
            const output = data.toString();
            stdout += output;
            console.log(`📋 Python: ${output.trim()}`);
        });

        pythonProcess.stderr.on('data', (data) => {
            const error = data.toString();
            stderr += error;
            console.log(`📋 Python Output: ${error.trim()}`);
        });

        pythonProcess.on('close', async (code) => {
            console.log(`🏁 Python process exited with code: ${code}`);

            if (code === 0) {
                // Success - check for output files
                try {
                    const outputDir = path.join(process.cwd(), 'scraped_courses', course_number);

                    const expectedFiles = [
                        `${course_number}_structured.json`,
                        `${course_number}_analysis_ready.json`,
                        `${course_number}_complete.json`,
                        `${course_number}.txt`
                    ];

                    const createdFiles = [];
                    let structuredDataExists = false;

                    for (const filename of expectedFiles) {
                        const filePath = path.join(outputDir, filename);
                        try {
                            await fs.access(filePath);
                            createdFiles.push(filename);
                            if (filename.endsWith('_structured.json')) {
                                structuredDataExists = true;
                            }
                        } catch (error) {
                            // File doesn't exist, skip
                        }
                    }

                    // Check if data was uploaded to database
                    let dataUploaded = false;
                    try {
                        // Add a small delay for database sync
                        await new Promise(resolve => setTimeout(resolve, 2000));

                        const { data: scrapingData, error } = await supabaseClient
                            .from('course_scraping_data')
                            .select('course_number, name')
                            .eq('course_number', course_number)
                            .maybeSingle();

                        console.log(`🔍 Database check result:`, scrapingData, error);
                        dataUploaded = !!scrapingData;
                    } catch (dbError) {
                        console.error('Database check error:', dbError);
                    }

                    console.log(`✅ Files created: ${createdFiles.join(', ')}`);
                    console.log(`💾 Database updated: ${dataUploaded}`);

                    res.json({
                        success: true,
                        course_number,
                        website_url,
                        files_created: createdFiles,
                        database_updated: dataUploaded,
                        force_reprocess_used: force_reprocess,
                        stdout: stdout,
                        message: `Successfully scraped ${course_number}`,
                        execution_time: new Date().toISOString()
                    });

                } catch (error) {
                    console.error('Error checking output files:', error);
                    res.json({
                        success: true,
                        course_number,
                        website_url,
                        files_created: [],
                        database_updated: false,
                        force_reprocess_used: force_reprocess,
                        stdout: stdout,
                        message: `Scraping completed but could not verify files: ${error.message}`,
                        execution_time: new Date().toISOString()
                    });
                }

            } else {
                // Failure
                console.error(`❌ Scraping failed for ${course_number}`);
                res.status(500).json({
                    success: false,
                    course_number,
                    website_url,
                    message: `Failed to scrape ${course_number}`,
                    error: stderr || stdout || 'Unknown error',
                    force_reprocess_used: force_reprocess,
                    exit_code: code
                });
            }
        });

        pythonProcess.on('error', (error) => {
            console.error(`❌ Failed to start Python process: ${error}`);
            res.status(500).json({
                success: false,
                course_number,
                website_url,
                message: 'Failed to start scraper process',
                error: error.message,
                force_reprocess_used: force_reprocess
            });
        });

        // Set a timeout (30 minutes max)
        setTimeout(() => {
            if (!pythonProcess.killed) {
                console.log(`⏰ Timeout: Killing Python process for ${course_number}`);
                pythonProcess.kill('SIGTERM');
                res.status(408).json({
                    success: false,
                    course_number,
                    website_url,
                    message: 'Scraping timed out after 30 minutes',
                    error: 'Timeout',
                    force_reprocess_used: force_reprocess
                });
            }
        }, 30 * 60 * 1000); // 30 minutes

    } catch (error) {
        console.error(`❌ API Error for ${course_number}:`, error);
        res.status(500).json({
            success: false,
            course_number: req.body.course_number,
            website_url: req.body.website_url,
            message: 'Internal server error',
            error: error.message,
            force_reprocess_used: req.body.force_reprocess || false
        });
    }
});

// Get scraping progress/status for a course - FIXED VERSION
app.get('/api/scraping-status/:courseNumber', requireSupabase, async (req, res) => {
  try {
    const { courseNumber } = req.params;

    // Check if scraping data exists - using correct supabaseClient
    const { data: scrapingData } = await supabaseClient
      .from('course_scraping_data')
      .select('course_number, last_updated, import_date')
      .eq('course_number', courseNumber)
      .maybeSingle();

    res.json({
      course_number: courseNumber,
      has_scraping_data: !!scrapingData,
      scraping_data_info: scrapingData,
      last_scraped: scrapingData?.last_updated || null
    });

  } catch (error) {
    console.error('Error getting scraping status:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get scraping status: ' + error.message
    });
  }
});



// List all courses with scraping data
app.get('/api/course-scraping', requireSupabase, async (req, res) => {
    try {
        const { data, error } = await supabaseClient
            .from('course_scraping_data')
            .select(`
                course_number,
                name,
                course_type,
                pricing_level,
                last_updated,
                import_date,
                pages_crawled
            `)
            .order('course_number');

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            count: data.length,
            data: data
        });

    } catch (error) {
        console.error('List error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Delete course scraping data
app.delete('/api/course-scraping/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        const { error } = await supabaseClient
            .from('course_scraping_data')
            .delete()
            .eq('course_number', courseNumber);

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            message: `Course scraping data deleted for course ${courseNumber}`
        });

    } catch (error) {
        console.error('Delete error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Clear all data endpoint
app.delete('/api/clear-all', requireSupabase, async (req, res) => {
    try {
        console.log('Starting data cleanup...');

        // Delete in reverse dependency order
        const tables = [
            'course_scraping_data',
            'course_comprehensive_analysis',
            'course_vector_attributes',
            'course_scores',
            'course_reviews',
            'primary_data',
            'review_urls',
            'google_places_data',
            'initial_course_upload'
        ];

        const results = {};

        for (const table of tables) {
            try {
                console.log(`Deleting ${table}...`);
                const { error } = await supabaseClient
                    .from(table)
                    .delete()
                    .neq('course_number', ''); // Delete all rows

                if (error) {
                    results[table] = { success: false, error: error.message };
                    console.error(`Error deleting from ${table}:`, error.message);
                } else {
                    results[table] = { success: true };
                    console.log(`Successfully cleared ${table}`);
                }
            } catch (error) {
                results[table] = { success: false, error: error.message };
                console.error(`Exception deleting from ${table}:`, error);
            }
        }

        const successCount = Object.values(results).filter(r => r.success).length;
        const totalTables = tables.length;

        res.json({
            success: successCount === totalTables,
            message: `Cleared ${successCount}/${totalTables} tables`,
            results
        });

    } catch (error) {
        console.error('Clear all data error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ADD THESE NEW ENDPOINTS HERE (before the health check)

// Get all courses from primary_data
app.get('/api/courses', requireSupabase, async (req, res) => {
    try {
        const { data, error } = await supabaseClient
            .from('primary_data')
            .select('course_number, course_name, website, state')  // ✅ Fixed: course_name instead of name
            .order('course_number');

        if (error) {
            console.error('Error getting courses from primary_data:', error);
            return res.status(500).json({ error: error.message });
        }

        console.log(`📋 Found ${data?.length || 0} courses in primary_data`);
        res.json(data || []);
    } catch (error) {
        console.error('Error getting courses:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get all courses from initial_course_upload
app.get('/api/initial-courses', requireSupabase, async (req, res) => {
    try {
        const { data, error } = await supabaseClient
            .from('initial_course_upload')
            .select('course_number, course_name, website_url')  // ✅ Fixed: course_name and website_url
            .order('course_number');

        if (error) {
            console.error('Error getting courses from initial_course_upload:', error);
            return res.status(500).json({ error: error.message });
        }

        console.log(`📋 Found ${data?.length || 0} courses in initial_course_upload`);
        res.json(data || []);
    } catch (error) {
        console.error('Error getting initial courses:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get single course from primary_data
app.get('/api/courses/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;
        console.log(`🔍 Looking up course ${courseNumber} in primary_data`);

        const { data, error } = await supabaseClient
            .from('primary_data')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code !== 'PGRST116') {
            console.error(`Error getting course ${courseNumber} from primary_data:`, error);
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            console.log(`❌ Course ${courseNumber} not found in primary_data`);
            return res.status(404).json({ error: 'Course not found in primary_data' });
        }

        console.log(`✅ Found course ${courseNumber} in primary_data`);
        res.json(data);
    } catch (error) {
        console.error('Error getting course:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get single course from initial_course_upload
app.get('/api/initial-courses/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;
        console.log(`🔍 Looking up course ${courseNumber} in initial_course_upload`);

        const { data, error } = await supabaseClient
            .from('initial_course_upload')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code !== 'PGRST116') {
            console.error(`Error getting course ${courseNumber} from initial_course_upload:`, error);
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            console.log(`❌ Course ${courseNumber} not found in initial_course_upload`);
            return res.status(404).json({ error: 'Course not found in initial_course_upload' });
        }

        console.log(`✅ Found course ${courseNumber} in initial_course_upload`);
        res.json(data);
    } catch (error) {
        console.error('Error getting initial course:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get courses that need scraping (for batch operations)
app.get('/api/courses-to-scrape', requireSupabase, async (req, res) => {
    try {
        const { state, limit } = req.query;

        let query = supabaseClient
            .from('initial_course_upload')
            .select('course_number, course_name, website_url, state_or_region')  // ✅ Fixed column names
            .not('website_url', 'is', null)
            .not('website_url', 'eq', '')
            .not('website_url', 'eq', 'N/A');

        if (state) {
            query = query.eq('state_or_region', state);  // ✅ Fixed: state_or_region instead of state
        }

        if (limit) {
            query = query.limit(parseInt(limit));
        }

        const { data, error } = await query.order('course_number');

        if (error) {
            console.error('Error getting courses to scrape:', error);
            return res.status(500).json({ error: error.message });
        }

        console.log(`📋 Found ${data?.length || 0} courses that need scraping`);
        res.json(data || []);
    } catch (error) {
        console.error('Error getting courses to scrape:', error);
        res.status(500).json({ error: error.message });
    }
});

// Upload scraping data endpoint (for the runner to call)
app.post('/api/course-scraping-data', requireSupabase, async (req, res) => {
    try {
        const { course_number, scraping_data, scraped_at } = req.body;

        if (!course_number || !scraping_data) {
            return res.status(400).json({
                success: false,
                error: 'course_number and scraping_data are required'
            });
        }

        console.log(`💾 Uploading scraping data for course ${course_number}`);

        // Transform the scraping data to match database schema
        const transformedData = transformScrapingData(scraping_data, course_number);

        // Upsert the data
        const { error } = await supabaseClient
            .from('course_scraping_data')
            .upsert(transformedData, {
                onConflict: 'course_number'
            });

        if (error) {
            console.error(`Database error uploading scraping data for ${course_number}:`, error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Successfully uploaded scraping data for course ${course_number}`);
        res.json({
            success: true,
            message: `Scraping data uploaded for course ${course_number}`,
            course_number
        });

    } catch (error) {
        console.error('Error uploading scraping data:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Pipeline update endpoint (for the runner to update status)
app.post('/api/pipeline/update', requireSupabase, async (req, res) => {
    try {
        const { course_number, current_step, progress_percent, status, step_details, error_message } = req.body;

        console.log(`📊 Pipeline update for ${course_number}: Step ${current_step}, Status: ${status}, Progress: ${progress_percent}%`);

        // For now, just log the pipeline update
        // You can implement actual pipeline table updates here if needed

        res.json({
            success: true,
            message: 'Pipeline status updated',
            course_number,
            status
        });

    } catch (error) {
        console.error('Error updating pipeline:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Fixed Google Places endpoint - using admin settings for script path
app.post('/api/run-google-places', requireSupabase, async (req, res) => {
    try {
        const { state, course, force = false, profile = 'default' } = req.body;

        console.log(`🌐 API: Starting Google Places enrichment`);
        if (course) {
            console.log(`🎯 Single course mode: ${course}`);
        } else if (state) {
            console.log(`🗺️ State mode: ${state}`);
        } else {
            console.log(`🌍 All courses mode`);
        }
        console.log(`🔥 Force update: ${force}`);

        // ✅ Get script path from admin settings
        const { data: adminSettings, error: settingsError } = await supabaseClient
            .from('admin_settings')
            .select('scripts_directory, pipeline_steps')
            .eq('profile_name', profile)
            .single();

        if (settingsError || !adminSettings) {
            console.error('❌ Could not load admin settings:', settingsError?.message);
            return res.status(500).json({
                success: false,
                error: 'Admin settings not found',
                message: `Could not load admin settings for profile '${profile}'. Please configure admin settings first.`
            });
        }

        // ✅ Find the Google Places step in pipeline_steps
        const googlePlacesStep = adminSettings.pipeline_steps.find(step =>
            step.name.toLowerCase().includes('google places') ||
            step.script === 'run_google_places_enrichment.py'
        );

        if (!googlePlacesStep) {
            console.error('❌ Google Places step not found in pipeline configuration');
            return res.status(500).json({
                success: false,
                error: 'Pipeline step not configured',
                message: 'Google Places Details step not found in admin settings pipeline configuration'
            });
        }

        if (googlePlacesStep.manual || !googlePlacesStep.script) {
            return res.status(400).json({
                success: false,
                error: 'Manual step',
                message: 'Google Places Details is configured as a manual step'
            });
        }

        // ✅ Build dynamic script path
        const runnerPath = path.join(adminSettings.scripts_directory, googlePlacesStep.script);
        console.log(`📂 Using script path from admin settings: ${runnerPath}`);

        // ✅ Verify script exists
        try {
            await fs.access(runnerPath);
            console.log(`✅ Script verified at: ${runnerPath}`);
        } catch (error) {
            console.error(`❌ Script not found at: ${runnerPath}`);
            return res.status(500).json({
                success: false,
                error: 'Script not found',
                message: `Google Places script not found at: ${runnerPath}. Please check admin settings.`,
                expected_path: runnerPath,
                scripts_directory: adminSettings.scripts_directory,
                script_name: googlePlacesStep.script
            });
        }

        // ✅ Build command arguments
        const args = [runnerPath];

        if (course) {
            args.push('--course', course);
        } else if (state) {
            args.push('--state', state);
        }

        if (force) {
            args.push('--force');
        }

        // ✅ Set up environment variables
        const env = Object.assign({}, process.env, {
            'API_URL': 'http://localhost:3000',
            'GOOGLE_PLACES_API_KEY': process.env.GOOGLE_PLACES_API_KEY || ''
        });

        console.log(`🚀 Command: python ${args.join(' ')}`);
        console.log(`🔧 Using admin settings profile: ${profile}`);
        console.log(`📁 Scripts directory: ${adminSettings.scripts_directory}`);

        const pythonProcess = spawn('python', args, {
            stdio: ['pipe', 'pipe', 'pipe'],
            cwd: path.dirname(runnerPath),  // ✅ Use script directory as working directory
            env: env
        });

        let stdout = '';
        let stderr = '';
        let hasResponded = false;

        pythonProcess.stdout.on('data', (data) => {
            const output = data.toString();
            stdout += output;
            console.log(`📋 STDOUT: ${output.trim()}`);
        });

        pythonProcess.stderr.on('data', (data) => {
            const error = data.toString();
            stderr += error;
            console.log(`📋 STDERR: ${error.trim()}`);
        });

        pythonProcess.on('close', async (code) => {
            if (hasResponded) return;
            hasResponded = true;

            console.log(`🏁 Process exited with code: ${code}`);

            if (code === 0) {
                // Success - check database
                try {
                    await new Promise(resolve => setTimeout(resolve, 2000));

                    let processedData;
                    if (course) {
                        const { data: singleCourseData } = await supabaseClient
                            .from('google_places_data')
                            .select('course_number, display_name, updated_at')
                            .eq('course_number', course)
                            .maybeSingle();
                        processedData = singleCourseData ? [singleCourseData] : [];
                    } else {
                        let query = supabaseClient
                            .from('google_places_data')
                            .select('course_number, display_name, updated_at')
                            .order('updated_at', { ascending: false })
                            .limit(10);

                        if (state) {
                            query = query.eq('state', state);
                        }

                        const { data: multipleData } = await query;
                        processedData = multipleData || [];
                    }

                    res.json({
                        success: true,
                        message: course
                            ? `Google Places enrichment completed successfully for course ${course}`
                            : `Google Places enrichment completed successfully`,
                        processed_courses: processedData?.length || 0,
                        recent_entries: processedData || [],
                        target_course: course || null,
                        target_state: state || null,
                        force_update_used: force,
                        script_path: runnerPath,
                        admin_profile: profile,
                        stdout: stdout,
                        execution_time: new Date().toISOString()
                    });

                } catch (dbError) {
                    console.error('Database check error:', dbError);
                    res.json({
                        success: true,
                        message: `Process completed but database check failed`,
                        script_path: runnerPath,
                        stdout: stdout,
                        stderr: stderr,
                        db_error: dbError.message
                    });
                }

            } else {
                res.status(500).json({
                    success: false,
                    message: `Google Places enrichment failed`,
                    error: stderr || stdout || 'Unknown error',
                    script_path: runnerPath,
                    stdout: stdout,
                    stderr: stderr,
                    exit_code: code,
                    command: `python ${args.join(' ')}`,
                    admin_profile: profile
                });
            }
        });

        pythonProcess.on('error', (error) => {
            if (hasResponded) return;
            hasResponded = true;

            console.error(`❌ Failed to start process: ${error}`);
            res.status(500).json({
                success: false,
                message: 'Failed to start Python process',
                error: error.message,
                script_path: runnerPath,
                command: `python ${args.join(' ')}`
            });
        });

        // Timeout after 15 minutes
        setTimeout(() => {
            if (!hasResponded) {
                hasResponded = true;
                if (!pythonProcess.killed) {
                    pythonProcess.kill('SIGTERM');
                }
                res.status(408).json({
                    success: false,
                    message: 'Process timed out',
                    script_path: runnerPath,
                    stdout: stdout,
                    stderr: stderr
                });
            }
        }, 15 * 60 * 1000);

    } catch (error) {
        console.error(`💥 Endpoint error:`, error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add this endpoint to your server.js file

// Google Places data endpoint - WITH FORCE LOGIC (to match review URLs behavior)
app.post('/api/google-places-data', requireSupabase, async (req, res) => {
    try {
        const {
            course_number,
            google_places_data,
            force = false  // ✅ Add force flag support
        } = req.body;

        if (!course_number || !google_places_data) {
            return res.status(400).json({
                success: false,
                error: 'course_number and google_places_data are required'
            });
        }

        console.log(`💾 Processing Google Places data for course: ${course_number}, Force: ${force}`);

        // ✅ Check if record already exists
        const { data: existingRecord, error: checkError } = await supabaseClient
            .from('google_places_data')
            .select('*')
            .eq('course_number', course_number)
            .maybeSingle();

        if (checkError) {
            console.error(`Database error checking existing Google Places data for ${course_number}:`, checkError);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: checkError.message
            });
        }

        // ✅ If record exists and force=false, skip update
        if (existingRecord && !force) {
            console.log(`⏭️ Course ${course_number} already has Google Places data and force=false, skipping`);
            return res.json({
                success: true,
                message: `Course ${course_number} already has Google Places data (use --force to overwrite)`,
                action: 'skipped',
                existing_data: existingRecord,
                course_number
            });
        }

        // ✅ Prepare data for insert/update
        const dataWithTimestamp = {
            ...google_places_data,
            course_number,
            updated_at: new Date().toISOString()
        };

        let data, error;

        if (existingRecord) {
            // ✅ Update existing record (force=true)
            console.log(`🔄 Updating existing Google Places data for course ${course_number} (force=true)`);
            ({ data, error } = await supabaseClient
                .from('google_places_data')
                .update(dataWithTimestamp)
                .eq('course_number', course_number)
                .select()
                .single());
        } else {
            // ✅ Insert new record
            console.log(`➕ Inserting new Google Places data for course ${course_number}`);
            ({ data, error } = await supabaseClient
                .from('google_places_data')
                .insert(dataWithTimestamp)
                .select()
                .single());
        }

        if (error) {
            console.error(`Database error saving Google Places data for ${course_number}:`, error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        const action = existingRecord ? 'updated' : 'created';
        console.log(`✅ Successfully ${action} Google Places data for course ${course_number}`);

        res.json({
            success: true,
            message: `Google Places data ${action} for course ${course_number}`,
            action: action,
            data: data,
            course_number,
            force_used: force
        });

    } catch (error) {
        console.error('Error saving Google Places data:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

console.log('✅ Google Places endpoint registered successfully');
// Get Google Places enrichment status
app.get('/api/google-places-status', requireSupabase, async (req, res) => {
    try {
        const { state } = req.query;

        let query = supabaseClient
            .from('google_places_data')
            .select('course_number, display_name, state, updated_at');

        if (state) {
            query = query.eq('state', state);
        }

        const { data: googlePlacesData, error: googleError } = await query
            .order('updated_at', { ascending: false });

        if (googleError) {
            throw googleError;
        }

        // Get total counts from primary_data vs google_places_data
        let totalQuery = supabaseClient
            .from('primary_data')
            .select('course_number, state');

        if (state) {
            totalQuery = totalQuery.eq('state', state);
        }

        const { data: allCourses, error: totalError } = await totalQuery;

        if (totalError) {
            throw totalError;
        }

        // Calculate statistics by state
        const stats = {};
        const googleCourseNumbers = new Set(googlePlacesData.map(g => g.course_number));

        allCourses.forEach(course => {
            const courseState = course.state || 'Unknown';
            if (!stats[courseState]) {
                stats[courseState] = {
                    total: 0,
                    processed: 0,
                    remaining: 0,
                    percentage: 0
                };
            }
            stats[courseState].total++;
            if (googleCourseNumbers.has(course.course_number)) {
                stats[courseState].processed++;
            }
        });

        // Calculate remaining and percentages
        Object.keys(stats).forEach(state => {
            const stateStats = stats[state];
            stateStats.remaining = stateStats.total - stateStats.processed;
            stateStats.percentage = stateStats.total > 0
                ? Math.round((stateStats.processed / stateStats.total) * 100)
                : 0;
        });

        res.json({
            success: true,
            statistics: stats,
            recent_entries: googlePlacesData.slice(0, 10),
            total_processed: googlePlacesData.length,
            filter_state: state || null,
            last_updated: new Date().toISOString()
        });

    } catch (error) {
        console.error('Error getting Google Places status:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get Google Places status: ' + error.message
        });
    }
});

// Get Google Places data for a specific course
app.get('/api/google-places-data/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        const { data, error } = await supabaseClient
            .from('google_places_data')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: 'No Google Places data found for this course',
                course_number: courseNumber
            });
        }

        if (error) {
            throw error;
        }

        res.json({
            success: true,
            data: data,
            course_number: courseNumber
        });

    } catch (error) {
        console.error('Error getting Google Places data:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get Google Places data: ' + error.message,
            course_number: req.params.courseNumber
        });
    }
});

// Delete Google Places data for a specific course
app.delete('/api/google-places-data/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        const { error } = await supabaseClient
            .from('google_places_data')
            .delete()
            .eq('course_number', courseNumber);

        if (error) {
            throw error;
        }

        res.json({
            success: true,
            message: `Google Places data deleted for course ${courseNumber}`,
            course_number: courseNumber
        });

    } catch (error) {
        console.error('Error deleting Google Places data:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to delete Google Places data: ' + error.message,
            course_number: req.params.courseNumber
        });
    }
});

// =============================================================================
// ADMIN SETTINGS API ENDPOINTS
// Add these endpoints to your server.js file
// =============================================================================

// Get admin settings by profile name (defaults to 'default')
app.get('/api/admin-settings', requireSupabase, async (req, res) => {
    try {
        const { profile = 'default' } = req.query;

        console.log(`📋 Getting admin settings for profile: ${profile}`);

        const { data, error } = await supabaseClient
            .from('admin_settings')
            .select('*')
            .eq('profile_name', profile)
            .single();

        if (error && error.code === 'PGRST116') {
            // No settings found, return defaults
            console.log(`ℹ️ No admin settings found for profile '${profile}', returning defaults`);

            const defaultSettings = {
                profile_name: profile,
                scripts_directory: '/Users/your-username/golf-scripts',
                states_directory: '/Users/your-username/golf-states',
                pipeline_steps: [
                    { id: 1, name: 'Initial Upload', script: '', manual: true },
                    { id: 2, name: 'Google Places Details', script: 'run_google_places_enrichment.py', manual: false },
                    { id: 3, name: 'GolfNow & GolfPass URLs', script: 'golf_url_finder.py', manual: false },
                    { id: 4, name: 'GolfNow Reviews', script: 'golfnow_reviews_batch.py', manual: false },
                    { id: 5, name: 'GolfPass Reviews', script: 'run_golfpass_scraper.py', manual: false },
                    { id: 6, name: 'Google Reviews', script: '', manual: true },
                    { id: 7, name: 'Score & Summarize Reviews', script: 'extract_text_insights.py', manual: false },
                    { id: 8, name: 'Combine Review Scores', script: 'combined_scores.py', manual: false },
                    { id: 9, name: 'Scrape Course Websites', script: 'database_integrated_runner.py', manual: false },
                    { id: 10, name: 'Download Scorecard Images', script: 'run_scorecard_extraction.py', manual: false },
                    { id: 11, name: 'Extract Tees & Ratings', script: 'scorecard_scraper.py', manual: false },
                    { id: 12, name: 'Collect Visuals & Weather', script: 'golf_course_visualizer.py', manual: false },
                    { id: 13, name: 'Strategic Analysis', script: 'comprehensive_golf_analysis.py', manual: false }
                ],
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            };

            return res.json({
                success: true,
                data: defaultSettings,
                is_default: true
            });
        }

        if (error) {
            console.error('Database error getting admin settings:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Found admin settings for profile '${profile}'`);
        res.json({
            success: true,
            data: data,
            is_default: false
        });

    } catch (error) {
        console.error('Error getting admin settings:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Save/update admin settings
app.post('/api/admin-settings', requireSupabase, async (req, res) => {
    try {
        const {
            profile_name = 'default',
            scripts_directory,
            states_directory,
            pipeline_steps
        } = req.body;

        console.log(`💾 Saving admin settings for profile: ${profile_name}`);

        // Validate required fields
        if (!scripts_directory || !states_directory) {
            return res.status(400).json({
                success: false,
                error: 'scripts_directory and states_directory are required'
            });
        }

        if (!Array.isArray(pipeline_steps)) {
            return res.status(400).json({
                success: false,
                error: 'pipeline_steps must be an array'
            });
        }

        const settingsRecord = {
            profile_name,
            scripts_directory: cleanValue(scripts_directory),
            states_directory: cleanValue(states_directory),
            pipeline_steps: pipeline_steps, // JSONB field
            updated_at: new Date().toISOString()
        };

        // Upsert the settings (insert or update)
        const { data, error } = await supabaseClient
            .from('admin_settings')
            .upsert(settingsRecord, {
                onConflict: 'profile_name',
                ignoreDuplicates: false
            })
            .select()
            .single();

        if (error) {
            console.error('Database error saving admin settings:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Successfully saved admin settings for profile '${profile_name}'`);
        res.json({
            success: true,
            message: `Admin settings saved for profile '${profile_name}'`,
            data: data
        });

    } catch (error) {
        console.error('Error saving admin settings:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get all admin settings profiles
app.get('/api/admin-settings/profiles', requireSupabase, async (req, res) => {
    try {
        console.log(`📋 Getting all admin settings profiles`);

        const { data, error } = await supabaseClient
            .from('admin_settings')
            .select('profile_name, created_at, updated_at')
            .order('created_at', { ascending: false });

        if (error) {
            console.error('Database error getting admin settings profiles:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Found ${data.length} admin settings profiles`);
        res.json({
            success: true,
            profiles: data,
            count: data.length
        });

    } catch (error) {
        console.error('Error getting admin settings profiles:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Delete admin settings profile
app.delete('/api/admin-settings/:profileName', requireSupabase, async (req, res) => {
    try {
        const { profileName } = req.params;

        if (profileName === 'default') {
            return res.status(400).json({
                success: false,
                error: 'Cannot delete the default profile'
            });
        }

        console.log(`🗑️ Deleting admin settings profile: ${profileName}`);

        const { error } = await supabaseClient
            .from('admin_settings')
            .delete()
            .eq('profile_name', profileName);

        if (error) {
            console.error('Database error deleting admin settings:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Successfully deleted admin settings profile '${profileName}'`);
        res.json({
            success: true,
            message: `Admin settings profile '${profileName}' deleted successfully`
        });

    } catch (error) {
        console.error('Error deleting admin settings:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get script path for a specific pipeline step
app.get('/api/admin-settings/script-path/:stepId', requireSupabase, async (req, res) => {
    try {
        const { stepId } = req.params;
        const { profile = 'default' } = req.query;

        console.log(`🔍 Getting script path for step ${stepId} in profile '${profile}'`);

        const { data, error } = await supabaseClient
            .from('admin_settings')
            .select('scripts_directory, pipeline_steps')
            .eq('profile_name', profile)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: `Admin settings profile '${profile}' not found`
            });
        }

        if (error) {
            console.error('Database error getting script path:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        // Find the step in pipeline_steps
        const step = data.pipeline_steps.find(s => s.id === parseInt(stepId));

        if (!step) {
            return res.status(404).json({
                success: false,
                error: `Pipeline step ${stepId} not found`
            });
        }

        if (step.manual || !step.script) {
            return res.json({
                success: true,
                step_id: parseInt(stepId),
                step_name: step.name,
                is_manual: true,
                script_path: null
            });
        }

        const scriptPath = `${data.scripts_directory}/${step.script}`;

        console.log(`✅ Script path for step ${stepId}: ${scriptPath}`);
        res.json({
            success: true,
            step_id: parseInt(stepId),
            step_name: step.name,
            is_manual: false,
            script_name: step.script,
            script_path: scriptPath,
            scripts_directory: data.scripts_directory
        });

    } catch (error) {
        console.error('Error getting script path:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get all script paths for pipeline steps
app.get('/api/admin-settings/script-paths', requireSupabase, async (req, res) => {
    try {
        const { profile = 'default' } = req.query;

        console.log(`📋 Getting all script paths for profile '${profile}'`);

        const { data, error } = await supabaseClient
            .from('admin_settings')
            .select('scripts_directory, states_directory, pipeline_steps')
            .eq('profile_name', profile)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: `Admin settings profile '${profile}' not found`
            });
        }

        if (error) {
            console.error('Database error getting script paths:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        // Build script paths for each step
        const scriptPaths = data.pipeline_steps.map(step => ({
            step_id: step.id,
            step_name: step.name,
            is_manual: step.manual,
            script_name: step.manual ? null : (step.script || null),
            script_path: (step.manual || !step.script) ? null : `${data.scripts_directory}/${step.script}`
        }));

        console.log(`✅ Generated script paths for ${scriptPaths.length} pipeline steps`);
        res.json({
            success: true,
            profile_name: profile,
            scripts_directory: data.scripts_directory,
            states_directory: data.states_directory,
            pipeline_steps: scriptPaths
        });

    } catch (error) {
        console.error('Error getting script paths:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Test admin settings configuration
app.post('/api/admin-settings/test', requireSupabase, async (req, res) => {
    try {
        const { profile = 'default' } = req.body;

        console.log(`🧪 Testing admin settings configuration for profile '${profile}'`);

        const { data, error } = await supabaseClient
            .from('admin_settings')
            .select('*')
            .eq('profile_name', profile)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: `Admin settings profile '${profile}' not found`
            });
        }

        if (error) {
            console.error('Database error testing admin settings:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        // Perform basic validation tests
        const tests = {
            profile_exists: !!data,
            has_scripts_directory: !!data.scripts_directory,
            has_states_directory: !!data.states_directory,
            has_pipeline_steps: Array.isArray(data.pipeline_steps) && data.pipeline_steps.length > 0,
            valid_pipeline_structure: true
        };

        // Validate pipeline steps structure
        if (data.pipeline_steps) {
            for (const step of data.pipeline_steps) {
                if (!step.id || !step.name || typeof step.manual !== 'boolean') {
                    tests.valid_pipeline_structure = false;
                    break;
                }
            }
        }

        const allTestsPassed = Object.values(tests).every(test => test === true);

        console.log(`${allTestsPassed ? '✅' : '❌'} Configuration test ${allTestsPassed ? 'passed' : 'failed'} for profile '${profile}'`);

        res.json({
            success: allTestsPassed,
            profile_name: profile,
            tests: tests,
            message: allTestsPassed
                ? 'All configuration tests passed'
                : 'Some configuration tests failed',
            data: data
        });

    } catch (error) {
        console.error('Error testing admin settings:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

console.log('✅ Admin Settings API endpoints registered successfully');

app.post('/api/run-pipeline-script', requireSupabase, async (req, res) => {
    try {
        const {
            course_number,
            script_name,
            script_path,
            state,
            force = false,
            description
        } = req.body;

        if (!script_name || !script_path) {
            return res.status(400).json({
                success: false,
                error: 'Missing required parameters: script_name, script_path'
            });
        }

        console.log('🚀 Executing pipeline script:', {
            course_number,
            script_name,
            script_path,
            state,
            force
        });

        // ✅ FIXED: Proper command building for different modes
        const pythonCommand = ['python3', script_path];

        // ✅ Handle different operation modes
        if (course_number && course_number !== 'BULK_OPERATION') {
            // Single course mode
            pythonCommand.push('--course', course_number);
            console.log(`🎯 Single course mode: ${course_number}`);
        } else if (course_number === 'BULK_OPERATION' && state) {
            // ✅ FIXED: Bulk operation with state filter
            pythonCommand.push('--state', state);
            console.log(`🗺️ Bulk operation for state: ${state}`);
        } else if (course_number === 'BULK_OPERATION') {
            // ✅ Bulk operation for all courses (no additional args needed)
            console.log(`🌍 Bulk operation for all courses`);
        } else {
            return res.status(400).json({
                success: false,
                error: 'Invalid course_number parameter. Use specific course number or "BULK_OPERATION"'
            });
        }

        // ✅ Add force flag if needed
        if (force) {
            pythonCommand.push('--force');
            console.log('🔥 Added --force flag to command');
        }

        console.log('📝 Running command:', pythonCommand.join(' '));

        // Set up environment variables
        const env = Object.assign({}, process.env, {
            'API_URL': process.env.API_URL || 'http://localhost:3000'
        });

        const pythonProcess = spawn(pythonCommand[0], pythonCommand.slice(1), {
            stdio: ['pipe', 'pipe', 'pipe'],
            cwd: path.dirname(script_path),
            env: env
        });

        let stdout = '';
        let stderr = '';
        let hasResponded = false;

        pythonProcess.stdout.on('data', (data) => {
            const output = data.toString();
            stdout += output;
            console.log(`📋 Script output: ${output.trim()}`);
        });

        pythonProcess.stderr.on('data', (data) => {
            const error = data.toString();
            stderr += error;
            console.log(`📋 Script stderr: ${error.trim()}`);
        });

        // Increase timeout to 15 minutes for complex scripts
        const timeout = setTimeout(() => {
            if (!hasResponded) {
                hasResponded = true;
                pythonProcess.kill('SIGTERM');
                res.status(408).json({
                    success: false,
                    error: 'Script execution timed out',
                    timeout_minutes: 15,
                    stdout: stdout,
                    stderr: stderr
                });
            }
        }, 15 * 60 * 1000);

        pythonProcess.on('close', (code) => {
            if (hasResponded) return;
            hasResponded = true;
            clearTimeout(timeout);

            console.log(`📊 Script ${script_name} finished with code: ${code}`);

            if (code === 0) {
                // ✅ Parse success metrics from stdout if available
                let processedCount = 0;
                let successfulCount = 0;
                let skippedCount = 0;

                try {
                    const lines = stdout.split('\n');
                    for (const line of lines) {
                        if (line.includes('Courses processed:')) {
                            const match = line.match(/Courses processed:\s*(\d+)/);
                            if (match) processedCount = parseInt(match[1]);
                        }
                        if (line.includes('Successful:')) {
                            const match = line.match(/Successful:\s*(\d+)/);
                            if (match) successfulCount = parseInt(match[1]);
                        }
                        if (line.includes('Skipped:') || line.includes('Courses skipped:')) {
                            const match = line.match(/(?:Skipped|Courses skipped):\s*(\d+)/);
                            if (match) skippedCount = parseInt(match[1]);
                        }
                    }
                } catch (parseError) {
                    console.log('Could not parse metrics from output');
                }

                res.json({
                    success: true,
                    message: `${description || script_name} completed successfully`,
                    course_number: course_number,
                    script_name: script_name,
                    operation_mode: course_number === 'BULK_OPERATION' ? 'bulk' : 'single',
                    state_filter: state || null,
                    force_used: force,
                    metrics: {
                        processed: processedCount,
                        successful: successfulCount,
                        skipped: skippedCount
                    },
                    stdout: stdout,
                    execution_time: new Date().toISOString()
                });
            } else {
                console.error('❌ Script error:', stderr);
                res.status(500).json({
                    success: false,
                    error: `Script execution failed with code ${code}`,
                    course_number: course_number,
                    script_name: script_name,
                    operation_mode: course_number === 'BULK_OPERATION' ? 'bulk' : 'single',
                    state_filter: state || null,
                    force_used: force,
                    stderr: stderr,
                    stdout: stdout,
                    exit_code: code
                });
            }
        });

        pythonProcess.on('error', (error) => {
            if (hasResponded) return;
            hasResponded = true;
            clearTimeout(timeout);

            console.error('💥 Process error:', error);
            res.status(500).json({
                success: false,
                error: `Failed to start script: ${error.message}`,
                course_number: course_number,
                script_name: script_name
            });
        });

    } catch (error) {
        console.error('💥 Pipeline script execution error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
// Add this endpoint to your server.js file (after the Google Places endpoints, around line 1200)

// Get available states from courses in database
app.get('/api/available-states', requireSupabase, async (req, res) => {
    try {
        console.log('📍 Getting available states from primary_data table...');

        // ✅ FIXED: Get unique states from primary_data table instead
        const { data: statesData, error } = await supabaseClient
            .from('primary_data')  // ← Changed from 'initial_course_upload'
            .select('state')       // ← Changed from 'state_or_region'
            .not('state', 'is', null)
            .not('state', 'eq', '');

        if (error) {
            console.error('Database error getting states:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        // Extract unique states and filter out empty/null values
        const uniqueStates = [...new Set(statesData.map(row => row.state))]  // ← Changed from 'state_or_region'
            .filter(state => state && state.trim())
            .sort();

        console.log(`✅ Found ${uniqueStates.length} unique states: ${uniqueStates.join(', ')}`);

        res.json({
            success: true,
            states: uniqueStates,
            count: uniqueStates.length,
            source: 'primary_data table'  // ← Updated source description
        });

    } catch (error) {
        console.error('Error getting available states:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Alternative endpoint that gets states from primary_data table as well
app.get('/api/available-states/all', requireSupabase, async (req, res) => {
    try {
        console.log('📍 Getting available states from all tables...');

        const statesFromTables = [];

        // Get states from initial_course_upload
        try {
            const { data: initialStates, error: initialError } = await supabaseClient
                .from('initial_course_upload')
                .select('state_or_region')
                .not('state_or_region', 'is', null)
                .not('state_or_region', 'eq', '');

            if (!initialError && initialStates) {
                statesFromTables.push(...initialStates.map(row => row.state_or_region));
                console.log(`📋 Found ${initialStates.length} state entries in initial_course_upload`);
            }
        } catch (error) {
            console.warn('Could not get states from initial_course_upload:', error.message);
        }

        // Get states from primary_data table if it exists
        try {
            const { data: primaryStates, error: primaryError } = await supabaseClient
                .from('primary_data')
                .select('state')
                .not('state', 'is', null)
                .not('state', 'eq', '');

            if (!primaryError && primaryStates) {
                statesFromTables.push(...primaryStates.map(row => row.state));
                console.log(`📋 Found ${primaryStates.length} state entries in primary_data`);
            }
        } catch (error) {
            console.warn('Could not get states from primary_data (table may not exist):', error.message);
        }

        // Extract unique states and filter out empty/null values
        const uniqueStates = [...new Set(statesFromTables)]
            .filter(state => state && state.trim())
            .sort();

        console.log(`✅ Found ${uniqueStates.length} unique states total: ${uniqueStates.join(', ')}`);

        res.json({
            success: true,
            states: uniqueStates,
            count: uniqueStates.length,
            source: 'initial_course_upload and primary_data tables'
        });

    } catch (error) {
        console.error('Error getting available states from all tables:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

console.log('✅ Available States API endpoints registered successfully');

// =============================================================================
// REVIEW URLS API ENDPOINTS
// Add these endpoints to your server.js file
// =============================================================================

// Get review URLs for a specific course
app.get('/api/review-urls/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        console.log(`🔍 Getting review URLs for course: ${courseNumber}`);

        const { data, error } = await supabaseClient
            .from('review_urls')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: 'No review URLs found for this course',
                course_number: courseNumber,
                has_urls: false
            });
        }

        if (error) {
            console.error('Database error getting review URLs:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Found review URLs for course ${courseNumber}`);
        res.json({
            success: true,
            data: data,
            course_number: courseNumber,
            has_urls: true
        });

    } catch (error) {
        console.error('Error getting review URLs:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Upsert review URLs for a course - WITH FORCE LOGIC
app.post('/api/review-urls/upsert', requireSupabase, async (req, res) => {
    try {
        const {
            course_number,
            course_name,
            golfnow_url,
            golfpass_url,
            force = false  // ✅ Check if force update is requested
        } = req.body;

        if (!course_number) {
            return res.status(400).json({
                success: false,
                error: 'course_number is required'
            });
        }

        console.log(`💾 Processing review URLs for course: ${course_number}, Force: ${force}`);

        // ✅ Check if record already exists
        const { data: existingRecord, error: checkError } = await supabaseClient
            .from('review_urls')
            .select('*')
            .eq('course_number', course_number)
            .maybeSingle();

        if (checkError) {
            console.error(`Database error checking existing URLs for ${course_number}:`, checkError);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: checkError.message
            });
        }

        // ✅ If record exists and force=false, skip update
        if (existingRecord && !force) {
            console.log(`⏭️ Course ${course_number} already has URLs and force=false, skipping`);
            return res.json({
                success: true,
                message: `Course ${course_number} already has URLs (use --force to overwrite)`,
                action: 'skipped',
                existing_data: existingRecord,
                course_number
            });
        }

        // ✅ Prepare data with correct column names for your schema
        const reviewUrlsRecord = {
            course_number,
            golf_now_url: cleanValue(golfnow_url),     // Match your schema
            golf_pass_url: cleanValue(golfpass_url),   // Match your schema
            updated_at: new Date().toISOString()
        };

        let data, error;

        if (existingRecord) {
            // ✅ Update existing record (force=true)
            console.log(`🔄 Updating existing URLs for course ${course_number} (force=true)`);
            ({ data, error } = await supabaseClient
                .from('review_urls')
                .update(reviewUrlsRecord)
                .eq('course_number', course_number)
                .select()
                .single());
        } else {
            // ✅ Insert new record
            console.log(`➕ Inserting new URLs for course ${course_number}`);
            ({ data, error } = await supabaseClient
                .from('review_urls')
                .insert(reviewUrlsRecord)
                .select()
                .single());
        }

        if (error) {
            console.error(`Database error saving URLs for ${course_number}:`, error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        const action = existingRecord ? 'updated' : 'created';
        console.log(`✅ Successfully ${action} review URLs for course ${course_number}`);

        res.json({
            success: true,
            message: `Review URLs ${action} for course ${course_number}`,
            action: action,
            data: data,
            course_number,
            force_used: force
        });

    } catch (error) {
        console.error('Error processing review URLs:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get courses that need URL processing (courses without review URLs or with incomplete URLs)
app.get('/api/courses-for-url-processing', requireSupabase, async (req, res) => {
    try {
        const { state, limit, force = false } = req.query;

        console.log(`📋 Getting courses for URL processing - State: ${state || 'all'}, Limit: ${limit || 'none'}, Force: ${force}`);

        // Start with all courses from primary_data
        let coursesQuery = supabaseClient
            .from('primary_data')
            .select('course_number, course_name, state, website');

        if (state) {
            coursesQuery = coursesQuery.eq('state', state);
        }

        if (limit) {
            coursesQuery = coursesQuery.limit(parseInt(limit));
        }

        const { data: allCourses, error: coursesError } = await coursesQuery.order('course_number');

        if (coursesError) {
            console.error('Error getting courses:', coursesError);
            return res.status(500).json({
                success: false,
                error: 'Database error getting courses',
                message: coursesError.message
            });
        }

        if (force === 'true') {
            // Return all courses if force is true
            console.log(`🔥 Force mode: returning all ${allCourses.length} courses`);
            return res.json({
                success: true,
                courses: allCourses,
                count: allCourses.length,
                filter_applied: 'none (force mode)',
                state_filter: state || null
            });
        }

        // Get existing review URLs
        let urlsQuery = supabaseClient
            .from('review_urls')
            .select('course_number, golfnow_url, golfpass_url, tripadvisor_url, yelp_url, google_maps_url');

        if (state) {
            // Need to join with courses to filter by state
            const courseNumbers = allCourses.map(course => course.course_number);
            urlsQuery = urlsQuery.in('course_number', courseNumbers);
        }

        const { data: existingUrls, error: urlsError } = await urlsQuery;

        if (urlsError) {
            console.error('Error getting existing URLs:', urlsError);
            return res.status(500).json({
                success: false,
                error: 'Database error getting existing URLs',
                message: urlsError.message
            });
        }

        // Create a map of existing URLs for quick lookup
        const urlsMap = new Map();
        if (existingUrls) {
            existingUrls.forEach(urlRecord => {
                urlsMap.set(urlRecord.course_number, urlRecord);
            });
        }

        // Filter courses that need URL processing
        const coursesNeedingUrls = allCourses.filter(course => {
            const existingUrlRecord = urlsMap.get(course.course_number);

            if (!existingUrlRecord) {
                // No URLs found at all
                return true;
            }

            // Check if URLs are incomplete (missing key review sites)
            const hasGolfNow = !!existingUrlRecord.golfnow_url;
            const hasGolfPass = !!existingUrlRecord.golfpass_url;
            const hasTripAdvisor = !!existingUrlRecord.tripadvisor_url;
            const hasYelp = !!existingUrlRecord.yelp_url;
            const hasGoogleMaps = !!existingUrlRecord.google_maps_url;

            // Consider incomplete if missing both GolfNow and GolfPass (the key review sites)
            return !hasGolfNow && !hasGolfPass;
        });

        console.log(`✅ Found ${coursesNeedingUrls.length} out of ${allCourses.length} courses that need URL processing`);

        res.json({
            success: true,
            courses: coursesNeedingUrls,
            count: coursesNeedingUrls.length,
            total_courses: allCourses.length,
            filter_applied: 'missing_key_urls',
            state_filter: state || null
        });

    } catch (error) {
        console.error('Error getting courses for URL processing:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Get URL processing status/statistics
app.get('/api/review-urls/status', requireSupabase, async (req, res) => {
    try {
        const { state } = req.query;

        console.log(`📊 Getting URL processing status - State: ${state || 'all'}`);

        // Get total courses
        let totalQuery = supabaseClient
            .from('primary_data')
            .select('course_number, state');

        if (state) {
            totalQuery = totalQuery.eq('state', state);
        }

        const { data: totalCourses, error: totalError } = await totalQuery;

        if (totalError) {
            throw totalError;
        }

        // Get courses with URLs
        let urlsQuery = supabaseClient
            .from('review_urls')
            .select('course_number, golfnow_url, golfpass_url, tripadvisor_url, yelp_url, google_maps_url, updated_at');

        if (state) {
            const courseNumbers = totalCourses.map(course => course.course_number);
            urlsQuery = urlsQuery.in('course_number', courseNumbers);
        }

        const { data: urlRecords, error: urlsError } = await urlsQuery.order('updated_at', { ascending: false });

        if (urlsError) {
            throw urlsError;
        }

        // Calculate statistics
        const stats = {
            total_courses: totalCourses.length,
            courses_with_urls: urlRecords.length,
            courses_without_urls: totalCourses.length - urlRecords.length,
            completion_percentage: totalCourses.length > 0
                ? Math.round((urlRecords.length / totalCourses.length) * 100)
                : 0
        };

        // Break down by URL type
        const urlTypeStats = {
            golfnow: urlRecords.filter(record => record.golfnow_url).length,
            golfpass: urlRecords.filter(record => record.golfpass_url).length,
            tripadvisor: urlRecords.filter(record => record.tripadvisor_url).length,
            yelp: urlRecords.filter(record => record.yelp_url).length,
            google_maps: urlRecords.filter(record => record.google_maps_url).length
        };

        // Recent activity
        const recentActivity = urlRecords.slice(0, 10);

        console.log(`✅ URL processing status: ${stats.courses_with_urls}/${stats.total_courses} courses (${stats.completion_percentage}%)`);

        res.json({
            success: true,
            statistics: stats,
            url_type_breakdown: urlTypeStats,
            recent_activity: recentActivity,
            state_filter: state || null,
            last_updated: new Date().toISOString()
        });

    } catch (error) {
        console.error('Error getting URL processing status:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get URL processing status: ' + error.message
        });
    }
});

// Get all review URLs (with pagination)
app.get('/api/review-urls', requireSupabase, async (req, res) => {
    try {
        const { state, limit = 100, offset = 0 } = req.query;

        console.log(`📋 Getting review URLs - State: ${state || 'all'}, Limit: ${limit}, Offset: ${offset}`);

        let query = supabaseClient
            .from('review_urls')
            .select('*')
            .order('updated_at', { ascending: false })
            .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);

        // If state filter is provided, we need to join with primary_data
        if (state) {
            // First get course numbers for the state
            const { data: stateCourses, error: stateError } = await supabaseClient
                .from('primary_data')
                .select('course_number')
                .eq('state', state);

            if (stateError) {
                throw stateError;
            }

            const courseNumbers = stateCourses.map(course => course.course_number);
            query = query.in('course_number', courseNumbers);
        }

        const { data, error } = await query;

        if (error) {
            console.error('Database error getting review URLs:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Retrieved ${data.length} review URL records`);
        res.json({
            success: true,
            data: data,
            count: data.length,
            limit: parseInt(limit),
            offset: parseInt(offset),
            state_filter: state || null
        });

    } catch (error) {
        console.error('Error getting review URLs:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Delete review URLs for a specific course
app.delete('/api/review-urls/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        console.log(`🗑️ Deleting review URLs for course: ${courseNumber}`);

        const { error } = await supabaseClient
            .from('review_urls')
            .delete()
            .eq('course_number', courseNumber);

        if (error) {
            console.error('Database error deleting review URLs:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        console.log(`✅ Successfully deleted review URLs for course ${courseNumber}`);
        res.json({
            success: true,
            message: `Review URLs deleted for course ${courseNumber}`,
            course_number: courseNumber
        });

    } catch (error) {
        console.error('Error deleting review URLs:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Bulk import review URLs
app.post('/api/import/review-urls', requireSupabase, async (req, res) => {
    try {
        const { data: urlsArray } = req.body;

        if (!Array.isArray(urlsArray)) {
            return res.status(400).json({
                success: false,
                error: 'Expected array of review URL objects'
            });
        }

        console.log(`Processing ${urlsArray.length} review URL records...`);

        let imported = 0;
        let errors = 0;
        const errorDetails = [];

        for (const urlData of urlsArray) {
            try {
                const courseNumber = urlData.course_number;

                if (!courseNumber) {
                    errors++;
                    errorDetails.push({
                        course: urlData.course_name || 'Unknown',
                        error: 'No course_number found'
                    });
                    continue;
                }

                const urlRecord = {
                    course_number: courseNumber,
                    course_name: cleanValue(urlData.course_name),
                    golfnow_url: cleanValue(urlData.golfnow_url),
                    golfpass_url: cleanValue(urlData.golfpass_url),
                    tripadvisor_url: cleanValue(urlData.tripadvisor_url),
                    yelp_url: cleanValue(urlData.yelp_url),
                    google_maps_url: cleanValue(urlData.google_maps_url),
                    other_urls: urlData.other_urls || [],
                    search_metadata: urlData.search_metadata || {},
                    updated_at: new Date().toISOString()
                };

                // Upsert the review URLs
                const { error } = await supabaseClient
                    .from('review_urls')
                    .upsert(urlRecord, {
                        onConflict: 'course_number',
                        ignoreDuplicates: false
                    });

                if (error) {
                    errors++;
                    errorDetails.push({
                        course: courseNumber,
                        error: error.message
                    });
                    console.error(`Review URLs import error for course ${courseNumber}: ${error.message}`);
                } else {
                    imported++;
                }

            } catch (error) {
                errors++;
                errorDetails.push({
                    course: urlData.course_number || urlData.course_name || 'Unknown',
                    error: error.message
                });
                console.error(`Error processing review URL data:`, error);
            }
        }

        res.json({
            success: true,
            message: `Review URLs import completed`,
            stats: {
                total: urlsArray.length,
                imported,
                errors
            },
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });

    } catch (error) {
        console.error('Review URLs import error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

console.log('✅ Review URLs API endpoints registered successfully');

// Add this endpoint to server.js if it doesn't exist
app.get('/api/course-scraping-data/:courseNumber', requireSupabase, async (req, res) => {
    try {
        const { courseNumber } = req.params;

        const { data, error } = await supabaseClient
            .from('course_scraping_data')
            .select('*')
            .eq('course_number', courseNumber)
            .single();

        if (error && error.code === 'PGRST116') {
            return res.status(404).json({
                success: false,
                error: 'No scraping data found for this course',
                course_number: courseNumber
            });
        }

        if (error) {
            console.error('Database error:', error);
            return res.status(500).json({
                success: false,
                error: 'Database error',
                message: error.message
            });
        }

        res.status(200).json({
            success: true,
            data: data,
            course_number: courseNumber
        });

    } catch (error) {
        console.error('Retrieval error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// Add this to your server.js - dedicated review URL endpoint
app.post('/api/run-review-urls', requireSupabase, async (req, res) => {
    try {
        const {
            course,
            state,
            force = false,
            max_courses  // ✅ Changed from limit to max_courses
        } = req.body;

        console.log(`🔍 Starting Review URL Finder`);

        // Get script path from admin settings
        const { data: adminSettings } = await supabaseClient
            .from('admin_settings')
            .select('scripts_directory, pipeline_steps')
            .eq('profile_name', 'default')
            .single();

        const reviewURLStep = adminSettings.pipeline_steps.find(step =>
            step.name.toLowerCase().includes('url') ||
            step.name.toLowerCase().includes('review')
        );

        const scriptPath = path.join(adminSettings.scripts_directory, reviewURLStep.script);

        // Build command
        const args = ['python3', scriptPath];

        if (course) {
            args.push('--course', course);
        } else if (state) {
            args.push('--state', state);
        }

        if (force) {
            args.push('--force');
        }

        if (max_courses) {
            args.push('--max-courses', max_courses.toString()); // ✅ Correct parameter name
        }

        // Execute script (similar to Google Places implementation)
        // ... rest of execution logic

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({
        success: true,
        message: 'Golf Data API is running',
        timestamp: new Date().toISOString(),
        supabaseConnected: !!supabaseClient
    });
});

// Error handling middleware
app.use((error, req, res, next) => {
    console.error('Unhandled error:', error);
    res.status(500).json({
        success: false,
        error: 'Internal server error'
    });
});

// 404 handler - serve index.html for non-API routes (SPA support)
app.use('*', (req, res) => {
    // If it's an API route, return 404 JSON
    if (req.originalUrl.startsWith('/api/')) {
        return res.status(404).json({
            success: false,
            error: 'Endpoint not found'
        });
    }

    // For non-API routes, serve the frontend
    res.sendFile(path.join(__dirname, 'golf-course-admin', 'index.html'));
});



// Start server
app.listen(PORT, () => {
    console.log(`Golf Data API server running on port ${PORT}`);
    console.log(`Available endpoints:`);
    console.log(`  POST /api/connect - Connect to Supabase`);
    console.log(`  POST /api/import/scores - Import course scores`);
    console.log(`  POST /api/import/vector-attributes - Import vector attributes`);
    console.log(`  POST /api/import/comprehensive-analysis - Import comprehensive analysis`);
    console.log(`  POST /api/import/reviews - Import course reviews`);
    console.log(`  POST /api/import/course-scraping - Import course scraping data (bulk)`);
    console.log(`  POST /api/import/course-scraping/:courseNumber - Import single course scraping data`);
    console.log(`  GET /api/course-scraping - List all courses with scraping data`);
    console.log(`  GET /api/course-scraping/:courseNumber - Get course scraping data`);
    console.log(`  DELETE /api/course-scraping/:courseNumber - Delete course scraping data`);
    console.log(`  DELETE /api/clear-all - Clear all data`);
    console.log(`  GET /api/health - Health check`);
    console.log(`  POST /api/run-website-scraper - Run website scraper`);
    console.log(`  GET /api/scraping-status/:courseNumber - Get scraping status`);
    console.log(`  POST /api/run-google-places - Run Google Places enrichment`);
});
connectServerToSupabase();
module.exports = app;
