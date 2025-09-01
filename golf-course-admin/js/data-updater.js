/**
 * DATABASE MANAGER MODULE
 * Handles all Supabase database operations
 */

export class DataUpdater {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.currentUrl = null;
    this.currentKey = null;
  }

  _ensureClient() {
    if (!this.client) throw new Error('Database not connected');
  }

  async connect(url, key) {
    try {
      if (!url || !key) throw new Error('Missing Supabase URL or Key');

      // Reuse existing good client
      if (this.client && this.isConnected && this.currentUrl === url && this.currentKey === key) {
        console.log('Already connected to this database');
        return true;
      }

      // If switching creds, clear old
      if (this.client && (this.currentUrl !== url || this.currentKey !== key)) {
        console.log('Switching to different database');
        this.client = null;
        this.isConnected = false;
      }

      // Resolve createClient from global/ESM
      const createClient =
        (typeof window !== 'undefined' && window.supabase && window.supabase.createClient)
          ? window.supabase.createClient
          : (typeof supabase !== 'undefined' && supabase.createClient)
            ? supabase.createClient
            : null;

      if (!createClient) {
        throw new Error('Supabase SDK not found. Load @supabase/supabase-js before app.js.');
      }

      if (!this.client) {
        this.client = createClient(url, key);
        this.currentUrl = url;
        this.currentKey = key;
      }

      // Smoke test
      const { error } = await this.client
        .from('initial_course_upload')
        .select('course_number')
        .limit(1);

      if (error) throw error;

      this.isConnected = true;
      return true;

    } catch (error) {
      this.isConnected = false;
      throw new Error(`Connection failed: ${error.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // Lists / lookups used by UI and updater
  // --------------------------------------------------------------------------

  async getCoursesList() {
    this._ensureClient();
    // Prefer primary_data; fallback to initial_course_upload
    const { data: primaryData, error: primaryErr } = await this.client
      .from('primary_data')
      .select('course_number, course_name, city, state, county')
      .order('course_number');

    if (!primaryErr && primaryData && primaryData.length > 0) return primaryData;

    const { data: initialRows, error: initialErr } = await this.client
      .from('initial_course_upload')
      .select('course_number, course_name, city, state:state_or_region, county')
      .order('course_number');

    if (initialErr) throw initialErr;
    return initialRows || [];
  }

  /**
   * Get courses (for pipeline manager) - returns courses with basic info
   */
  async getCourses() {
    this._ensureClient();
    try {
      // Try primary first
      const { data: primaryData, error: primaryErr } = await this.client
        .from('primary_data')
        .select('course_number, course_name, city, state')
        .order('course_number');

      if (!primaryErr && primaryData && primaryData.length > 0) {
        return primaryData;
      }

      // Fallback to initial upload (use colon alias to avoid "asstate")
      const { data: initialRows, error: initialErr } = await this.client
        .from('initial_course_upload')
        .select('course_number, course_name, city, state:state_or_region')
        .order('course_number');

      if (initialErr) throw initialErr;
      return initialRows || [];

    } catch (error) {
      console.error('Error getting courses:', error);
      throw error;
    }
  }

  async getInitialCourseUpload() {
    this._ensureClient();
    const { data, error } = await this.client
      .from('initial_course_upload')
      .select('course_number, course_name, city, state_or_region, created_at')
      .order('course_number');

    if (error) throw error;
    return data || [];
  }

  // --------------------------------------------------------------------------
  // Aggregated loader used by app
  // --------------------------------------------------------------------------
  async loadAllCourseData(courseNumber) {
    this._ensureClient();
    try {
      const [primary, scores, vector, analysis, reviews, usgolf, scraping, reviewUrls, teesAndPars] = await Promise.all([
        this.getCurrentPrimaryData(courseNumber).catch(err => {
          console.warn(`Primary data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadScoresData(courseNumber).catch(err => {
          console.warn(`Scores data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadVectorData(courseNumber).catch(err => {
          console.warn(`Vector data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadAnalysisData(courseNumber).catch(err => {
          console.warn(`Analysis data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadReviewsData(courseNumber).catch(err => {
          console.warn(`Reviews data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadUSGolfData(courseNumber).catch(err => {
          console.warn(`USGolf data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.getCourseInfoData(courseNumber).catch(err => {
          console.warn(`Course info data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadReviewUrlsData(courseNumber).catch(err => {
          console.warn(`Review URLs data error for ${courseNumber}:`, err.message);
          return null;
        }),
        this.loadTeesAndParsData(courseNumber).catch(err => {
          console.warn(`Tees and pars data error for ${courseNumber}:`, err.message);
          return { tees: [], pars: null };
        })
      ]);

      return {
        primary,
        scores,
        vector,
        analysis,
        reviews,
        usgolf,
        scraping,
        reviewUrls,
        teesAndPars
      };
    } catch (error) {
      console.error(`LoadAllCourseData error for ${courseNumber}:`, error);
      throw error;
    }
  }

  // --------------------------------------------------------------------------
  // Tees & Pars
  // --------------------------------------------------------------------------
  async loadTeesAndParsData(courseNumber) {
    this._ensureClient();
    try {
      const [tees, pars] = await Promise.all([
        this.loadTeesData(courseNumber).catch(err => {
          console.warn(`Tees data error for ${courseNumber}:`, err.message);
          return [];
        }),
        this.loadParsData(courseNumber).catch(err => {
          console.warn(`Pars data error for ${courseNumber}:`, err.message);
          return null;
        })
      ]);
      return { tees, pars };
    } catch (error) {
      console.error(`LoadTeesAndParsData error for ${courseNumber}:`, error);
      throw error;
    }
  }

  async loadTeesData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_tees')
      .select('*')
      .eq('course_number', courseNumber)
      .eq('data_group', 'primary_data');

    if (error && error.code !== 'PGRST116') throw error;

    const sorted = (data || []).sort((a, b) => {
      const ya = parseInt(a.total_yardage) || 0;
      const yb = parseInt(b.total_yardage) || 0;
      return yb - ya;
    });
    return sorted;
  }

  async loadParsData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_pars')
      .select('*')
      .eq('course_number', courseNumber)
      .eq('data_group', 'primary_data')
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  // --------------------------------------------------------------------------
  // Individual tables
  // --------------------------------------------------------------------------
  async loadScoresData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_scores')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async loadVectorData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_vector_attributes')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async loadAnalysisData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_comprehensive_analysis')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async loadReviewsData(courseNumber) {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('course_reviews')
        .select('*')
        .eq('course_number', courseNumber)
        .single();

      if (error && error.code !== 'PGRST116') {
        console.warn(`Reviews data not found for ${courseNumber}:`, error.message);
        return null;
      }
      return data || null;
    } catch (err) {
      console.warn(`Reviews data fetch failed for ${courseNumber}:`, err.message);
      return null;
    }
  }

  async loadPrimaryData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('primary_data')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async loadUSGolfData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('initial_course_upload')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async loadScrapingData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_scraping_data')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async getCurrentPrimaryData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('primary_data')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async getGooglePlacesData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('google_places_data')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  async getCourseInfoData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('course_scraping_data')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || {};
  }

  // --------------------------------------------------------------------------
  // Writes / updates
  // --------------------------------------------------------------------------
  async updatePrimaryData(courseNumber, updates) {
    this._ensureClient();
    const { error } = await this.client
      .from('primary_data')
      .update(updates)
      .eq('course_number', courseNumber);

    if (error) throw error;
    return true;
  }

  async updateField(table, courseNumber, fieldName, value, isManual = false) {
    this._ensureClient();
    const updateData = { [fieldName]: value };

    if (table !== 'initial_course_upload' && isManual) {
      updateData[fieldName + '_source'] = 'manual';
      updateData[fieldName + '_updated_at'] = new Date().toISOString();
    }

    const { error } = await this.client
      .from(table)
      .update(updateData)
      .eq('course_number', courseNumber);

    if (error) throw error;
    return true;
  }

  async insertPrimaryData(recordData) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('primary_data')
      .insert(recordData)
      .select()
      .single();

    if (error) throw new Error(`Failed to insert primary data: ${error.message}`);
    return data;
  }

  // --------------------------------------------------------------------------
  // Pipeline status helpers
  // --------------------------------------------------------------------------
  async getPipelineStatus(courseNumber) {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .select('*')
        .eq('course_number', courseNumber)
        .single();

      if (error && error.code !== 'PGRST116') throw error;
      return data || null;
    } catch (error) {
      console.error(`Error getting pipeline status for ${courseNumber}:`, error);
      throw error;
    }
  }

  async getAllPipelineStatuses() {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .select('*')
        .order('last_updated', { ascending: false });

      if (error) throw error;
      return data || [];
    } catch (error) {
      console.error('Error getting all pipeline statuses:', error);
      throw error;
    }
  }

  async updatePipelineStatus(courseNumber, statusUpdates) {
    this._ensureClient();
    try {
      const updateData = { ...statusUpdates, last_updated: new Date().toISOString() };
      const { data, error } = await this.client
        .from('pipeline_status')
        .update(updateData)
        .eq('course_number', courseNumber)
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error(`Error updating pipeline status for ${courseNumber}:`, error);
      throw error;
    }
  }

  async initializePipelineStatus(courseNumber) {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .insert({
          course_number: courseNumber,
          current_step: 1,
          progress_percent: 0,
          status: 'pending',
          last_updated: new Date().toISOString(),
          step_details: {},
          error_message: null
        })
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      if (error.code === '23505') {
        return await this.getPipelineStatus(courseNumber);
      }
      console.error(`Error initializing pipeline status for ${courseNumber}:`, error);
      throw error;
    }
  }

  async setPipelineError(courseNumber, errorMessage) {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .update({
          status: 'error',
          error_message: errorMessage,
          last_updated: new Date().toISOString()
        })
        .eq('course_number', courseNumber)
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error(`Error setting pipeline error for ${courseNumber}:`, error);
      throw error;
    }
  }

  async clearPipelineError(courseNumber) {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .update({
          status: 'pending',
          error_message: null,
          last_updated: new Date().toISOString()
        })
        .eq('course_number', courseNumber)
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error(`Error clearing pipeline error for ${courseNumber}:`, error);
      throw error;
    }
  }

  // --------------------------------------------------------------------------
  // Course create / lookup helpers
  // --------------------------------------------------------------------------
  async addCourse(courseData) {
    this._ensureClient();
    try {
      // Prevent duplicates
      const existingCourse = await this.getCourseByNumber(courseData.course_number);
      if (existingCourse) throw new Error(`Course ${courseData.course_number} already exists`);

      const insertData = {
        course_number:        courseData.course_number,
        course_name:          courseData.course_name || courseData.course_number,
        street_address:       courseData.street_address || null,
        city:                 courseData.city || null,
        county:               courseData.county || null,
        state_or_region:      courseData.state || courseData.state_or_region || null, // canonical
        zip_code:             courseData.zip_code || null,
        phone_number:         courseData.phone || courseData.phone_number || null,
        website_url:          courseData.website || courseData.website_url || null,
        created_at:           new Date().toISOString()
      };

      const { data, error } = await this.client
        .from('initial_course_upload')
        .insert(insertData)
        .select()
        .single();

      if (error) throw error;
      return data;

    } catch (error) {
      console.error(`Error adding course ${courseData.course_number}:`, error);
      throw error;
    }
  }

  async getCourseByNumber(courseNumber) {
    this._ensureClient();
    try {
      const { data: primaryData } = await this.client
        .from('primary_data')
        .select('*')
        .eq('course_number', courseNumber)
        .single();

      if (primaryData) return primaryData;

      const { data: initialData } = await this.client
        .from('initial_course_upload')
        .select('*')
        .eq('course_number', courseNumber)
        .single();

      return initialData || null;

    } catch (_err) {
      return null; // treat not found as null
    }
  }

  async getPipelineStatistics() {
    this._ensureClient();
    try {
      const { data, error } = await this.client
        .from('pipeline_status')
        .select('status');

      if (error) throw error;

      const stats = { total: data.length, pending: 0, running: 0, complete: 0, error: 0 };
      data.forEach(r => {
        const s = r.status || 'pending';
        stats[s] = (stats[s] || 0) + 1;
      });
      return stats;

    } catch (error) {
      console.error('Error getting pipeline statistics:', error);
      return { total: 0, pending: 0, running: 0, complete: 0, error: 0 };
    }
  }

  async bulkUpdatePipelineStatus(courseNumbers, statusUpdates) {
    this._ensureClient();
    try {
      const updateData = { ...statusUpdates, last_updated: new Date().toISOString() };
      const { data, error } = await this.client
        .from('pipeline_status')
        .update(updateData)
        .in('course_number', courseNumbers)
        .select();

      if (error) throw error;
      return data;

    } catch (error) {
      console.error('Error bulk updating pipeline statuses:', error);
      throw error;
    }
  }

  async deletePipelineStatus(courseNumbers) {
    this._ensureClient();
    try {
      const { error } = await this.client
        .from('pipeline_status')
        .delete()
        .in('course_number', Array.isArray(courseNumbers) ? courseNumbers : [courseNumbers]);

      if (error) throw error;
      return true;

    } catch (error) {
      console.error('Error deleting pipeline statuses:', error);
      throw error;
    }
  }

  // --------------------------------------------------------------------------
  // Field-type helpers used by DataUpdater
  // --------------------------------------------------------------------------
  getFieldMappings() {
    return {
      google_places_data: {
        display_name: 'course_name',
        formatted_address: 'formatted_address',
        street_number: 'street_number',
        route: 'route',
        street_address: 'street_address',
        city: 'city',
        state: 'state',
        zip_code: 'zip_code',
        county: 'county',
        country: 'country',
        phone: 'phone',
        website: 'website',
        latitude: 'latitude',
        longitude: 'longitude',
        place_id: 'place_id',
        user_rating_count: 'google_review_count',
        primary_type: 'primary_type',
        opening_hours: 'opening_hours',
        photo_reference: 'photo_reference',
        google_maps_link: 'google_maps_link'
      },

      course_scraping_data: {
        name: 'course_name',
        address: 'street_address',
        phone: 'phone',
        email: 'email_address',
        website: 'website',
        architect: 'architect',
        year_built: 'year_built_founded',
        course_type: 'course_type',
        is_18_hole: 'is_18_hole',
        is_9_hole: 'is_9_hole',
        is_par_3_course: 'is_par_3_course',
        is_executive_course: 'is_executive_course',
        has_ocean_views: 'has_ocean_views',
        has_scenic_views: 'has_scenic_views',
        signature_holes: 'signature_holes',
        has_driving_range: 'has_driving_range',
        driving_range_details: 'driving_range_details',
        has_practice_green: 'has_practice_green',
        practice_green_details: 'practice_green_details',
        has_short_game_area: 'has_short_game_area',
        short_game_area_details: 'short_game_area_details',
        has_clubhouse: 'has_clubhouse',
        clubhouse_details: 'clubhouse_details',
        has_pro_shop: 'has_pro_shop',
        pro_shop_details: 'pro_shop_details',
        has_locker_rooms: 'has_locker_rooms',
        locker_room_details: 'locker_room_details',
        has_showers: 'has_showers',
        shower_details: 'shower_details',
        has_beverage_cart: 'has_beverage_cart',
        beverage_cart_details: 'beverage_cart_details',
        has_banquet_facilities: 'has_banquet_facilities',
        banquet_facilities_details: 'banquet_facilities_details',
        pricing_level: 'pricing_level',
        pricing_level_description: 'pricing_level_description',
        typical_18_hole_rate: 'typical_18_hole_rate',
        pricing_information: 'pricing_information',
        scorecard_url: 'scorecard_url',
        about_url: 'about_url',
        membership_url: 'membership_url',
        tee_time_url: 'tee_time_url',
        rates_url: 'rates_url',
        facebook_url: 'facebook_url',
        instagram_url: 'instagram_url',
        twitter_url: 'twitter_url',
        youtube_url: 'youtube_url',
        tiktok_url: 'tiktok_url',
        course_description: 'course_description',
        food_beverage_options: 'food_beverage_options',
        food_beverage_description: 'food_beverage_description',
        course_history_general: 'course_history_general',
        notable_events: 'notable_events',
        design_features: 'design_features',
        recognitions: 'recognitions',
        rankings: 'rankings',
        certifications: 'certifications',
        amateur_tournaments: 'amateur_tournaments',
        professional_tournaments: 'professional_tournaments',
        charity_events: 'charity_events',
        event_contact: 'event_contact',
        course_policies: 'course_policies',
        sustainability_general: 'sustainability_general',
        sustainability_certifications: 'sustainability_certifications',
        sustainability_practices: 'sustainability_practices'
      },

      review_urls: {
        golf_now_url: 'golfnow_url',
        golf_pass_url: 'golfpass_url'
      },

      course_reviews: {
        course_name: 'course_name',
        total_reviews: 'total_reviews',
        overall_rating: 'overall_rating',
        recommend_percent: 'recommend_percent',
        golfnow_reviews: 'golfnow_reviews',
        golfpass_reviews: 'golfpass_reviews',
        conditions_rating: 'conditions_rating',
        value_rating: 'value_rating',
        friendliness_rating: 'friendliness_rating',
        pace_rating: 'pace_rating',
        amenities_rating: 'amenities_rating',
        difficulty_rating: 'difficulty_rating',
        layout_rating: 'layout_rating',
        fairways_score: 'fairways_score',
        greens_score: 'greens_score',
        bunkers_score: 'bunkers_score',
        tee_boxes_score: 'tee_boxes_score',
        shot_variety_score: 'shot_variety_score',
        signature_holes_score: 'signature_holes_score',
        water_ob_placement_score: 'water_ob_placement_score',
        overall_feel_scenery_score: 'overall_feel_scenery_score',
        green_complexity_score: 'green_complexity_score',
        staff_friendliness_score: 'staff_friendliness_score',
        green_fees_quality_score: 'green_fees_quality_score',
        replay_value_score: 'replay_value_score',
        ease_walking_score: 'ease_walking_score',
        pace_play_score: 'pace_play_score',
        availability_score: 'availability_score',
        form_category_averages: 'form_category_averages',
        text_insight_averages: 'text_insight_averages',
        top_text_themes: 'top_text_themes',
        reviews_by_source: 'reviews_by_source'
      },

      course_scores: {
        course_name: 'course_name',
        total_score: 'course_score',
        max_score: 'max_score',
        percentage: 'course_score_percentage',
        course_conditions_score: 'course_conditions_score',
        course_conditions_max: 'course_conditions_max',
        course_layout_design_score: 'course_layout_design_score',
        course_layout_design_max: 'course_layout_design_max',
        amenities_score: 'amenities_score',
        amenities_max: 'amenities_max',
        player_experience_score: 'player_experience_score',
        player_experience_max: 'player_experience_max',
        scores: 'scores',
        detailed_scores_with_explanations: 'detailed_scores_with_explanations',
        scoring_version: 'scoring_version'
      },

      course_vector_attributes: {
        beginner_friendly_score: 'beginner_friendly_score',
        ball_loss_risk_score: 'ball_loss_risk_score',
        open_course_feeling: 'open_course_feeling',
        natural_character_score: 'natural_character_score',
        water_course_score: 'water_course_score',
        ball_findability_score: 'ball_findability_score',
        tree_coverage_density: 'tree_coverage_density',
        visual_tightness: 'visual_tightness',
        natural_integration: 'natural_integration',
        water_prominence: 'water_prominence',
        course_openness: 'course_openness',
        terrain_visual_complexity: 'terrain_visual_complexity',
        elevation_feature_prominence: 'elevation_feature_prominence',
        design_style_category: 'design_style_category',
        routing_style: 'routing_style'
      },

      course_comprehensive_analysis: {
        course_name: 'course_name',
        analysis_timestamp: 'analysis_timestamp',
        latitude: 'latitude',
        longitude: 'longitude',
        total_holes: 'total_holes',
        total_bunkers: 'total_bunkers',
        left_biased_holes: 'left_biased_holes',
        right_biased_holes: 'right_biased_holes',
        course_bunker_bias: 'course_bunker_bias',
        left_doglegs: 'left_doglegs',
        right_doglegs: 'right_doglegs',
        total_doglegs: 'total_doglegs',
        dogleg_balance: 'dogleg_balance',
        overall_handedness_advantage: 'overall_handedness_advantage',
        short_hitter_safety_pct: 'short_hitter_safety_pct',
        average_hitter_safety_pct: 'average_hitter_safety_pct',
        long_hitter_safety_pct: 'long_hitter_safety_pct',
        weather_data_source: 'weather_data_source',
        weather_years_analyzed: 'weather_years_analyzed',
        total_days_analyzed: 'total_days_analyzed',
        avg_temp_c: 'avg_temp_c',
        golf_season_avg_temp_c: 'golf_season_avg_temp_c',
        golf_season_length_months: 'golf_season_length_months',
        golf_season_type: 'golf_season_type',
        rainy_days_pct: 'rainy_days_pct',
        heavy_rain_days_pct: 'heavy_rain_days_pct',
        avg_wind_kph: 'avg_wind_kph',
        windy_days_pct: 'windy_days_pct',
        best_golf_month: 'best_golf_month',
        best_golf_score: 'best_golf_score',
        worst_golf_month: 'worst_golf_month',
        worst_golf_score: 'worst_golf_score',
        total_elevation_change_m: 'total_elevation_change_m',
        average_elevation_change_m: 'average_elevation_change_m',
        max_single_hole_change_m: 'max_single_hole_change_m',
        uphill_holes: 'uphill_holes',
        downhill_holes: 'downhill_holes',
        flat_holes: 'flat_holes',
        extreme_difficulty_holes: 'extreme_difficulty_holes',
        challenging_difficulty_holes: 'challenging_difficulty_holes',
        most_challenging_hole: 'most_challenging_hole',
        strategic_analysis: 'strategic_analysis',
        weather_analysis: 'weather_analysis',
        elevation_analysis: 'elevation_analysis'
      },

      initial_course_upload: {
        course_name: 'course_name',
        street_address: 'street_address',
        city: 'city',
        county: 'county',
        state_or_region: 'state',
        zip_code: 'zip_code',
        phone_number: 'phone',
        website_url: 'website',
        architect: 'architect',
        year_built_founded: 'year_built_founded',
        status_type: 'status_type',
        email_address: 'email_address',
        total_par: 'total_par',
        total_holes: 'total_holes',
        course_rating: 'course_rating',
        slope_rating: 'slope_rating',
        total_length: 'total_length',
        latitude: 'latitude',
        longitude: 'longitude',

        // SEO & marketing: highest priority from initial upload
        meta_title: 'meta_title',
        meta_data_description: 'meta_description',
        open_graph_title: 'open_graph_title',
        open_graph_description: 'open_graph_description',
        course_image_url: 'course_image_url',
        alt_image_text: 'alt_image_text',

        // Certifications
        acsp_certification: 'acsp_certification',
        century_club: 'century_club',
        century_club_group: 'century_club_group',
        monarchs_in_the_rough: 'monarchs_in_the_rough'
      }
    };
  }

  getPriorityOrder() {
    return [
      'google_places_data',
      'review_urls',
      'course_scraping_data',
      'course_reviews',
      'course_scores',
      'course_vector_attributes',
      'course_comprehensive_analysis',
      'initial_course_upload'
    ];
  }

  async loadReviewUrlsData(courseNumber) {
    this._ensureClient();
    const { data, error } = await this.client
      .from('review_urls')
      .select('*')
      .eq('course_number', courseNumber)
      .single();

    if (error && error.code !== 'PGRST116') throw error;
    return data || null;
  }

  getBooleanFields() {
    return [
      'is_18_hole', 'is_9_hole', 'is_par_3_course', 'is_executive_course',
      'has_ocean_views', 'has_scenic_views', 'par_3_course',
      'has_driving_range', 'has_practice_green', 'has_short_game_area',
      'has_clubhouse', 'has_pro_shop', 'has_locker_rooms', 'has_showers',
      'has_beverage_cart', 'has_banquet_facilities',
      'driving_range', 'putting_green', 'pro_shop', 'club_house',
      'acsp_certification', 'monarchs_in_the_rough'
    ];
  }

  getNumberFields() {
    return [
      'total_holes', 'total_par', 'course_rating', 'slope_rating', 'total_length',
      'year_built_founded', 'latitude', 'longitude', 'zip_code',
      'google_review_count', 'total_reviews',
      'overall_rating', 'recommend_percent', 'golfnow_reviews', 'golfpass_reviews',
      'conditions_rating', 'value_rating', 'friendliness_rating', 'pace_rating',
      'amenities_rating', 'difficulty_rating', 'layout_rating', 'google_rating',
      'fairways_score', 'greens_score', 'bunkers_score', 'tee_boxes_score',
      'shot_variety_score', 'signature_holes_score', 'water_ob_placement_score',
      'overall_feel_scenery_score', 'green_complexity_score', 'staff_friendliness_score',
      'green_fees_quality_score', 'replay_value_score', 'ease_walking_score',
      'pace_play_score', 'availability_score',
      'course_score', 'max_score', 'course_score_percentage',
      'course_conditions_score', 'course_conditions_max',
      'course_layout_design_score', 'course_layout_design_max',
      'amenities_score', 'amenities_max',
      'player_experience_score', 'player_experience_max',
      'beginner_friendly_score', 'ball_loss_risk_score', 'open_course_feeling',
      'natural_character_score', 'water_course_score', 'ball_findability_score',
      'tree_coverage_density', 'visual_tightness', 'natural_integration',
      'water_prominence', 'course_openness', 'terrain_visual_complexity',
      'elevation_feature_prominence',
      'total_bunkers', 'left_biased_holes', 'right_biased_holes',
      'left_doglegs', 'right_doglegs', 'total_doglegs',
      'short_hitter_safety_pct', 'average_hitter_safety_pct', 'long_hitter_safety_pct',
      'total_days_analyzed', 'avg_temp_c', 'golf_season_avg_temp_c',
      'golf_season_length_months', 'rainy_days_pct', 'heavy_rain_days_pct',
      'avg_wind_kph', 'windy_days_pct', 'best_golf_score', 'worst_golf_score',
      'total_elevation_change_m', 'average_elevation_change_m', 'max_single_hole_change_m',
      'uphill_holes', 'downhill_holes', 'flat_holes',
      'extreme_difficulty_holes', 'challenging_difficulty_holes',
      'pricing_level', 'typical_18_hole_rate',
      'weather_years_analyzed', 'total_days_analyzed'
    ];
  }

  getTextAreaFields() {
    return [
      'course_description', 'difficulty_summary', 'food_and_drink', 'history',
      'sustainability', 'course_review', 'directions', 'amenities',
      'course_images', 'formatted_address', 'opening_hours',
      'food_beverage_options', 'food_beverage_description', 'signature_holes',
      'pricing_information', 'course_policies', 'course_history_general',
      'notable_events', 'design_features', 'recognitions', 'rankings',
      'certifications', 'amateur_tournaments', 'professional_tournaments',
      'charity_events', 'sustainability_general', 'sustainability_certifications',
      'sustainability_practices', 'event_contact', 'pricing_level_description',
      'driving_range_details', 'practice_green_details', 'short_game_area_details',
      'clubhouse_details', 'pro_shop_details', 'locker_room_details',
      'shower_details', 'beverage_cart_details', 'banquet_facilities_details',
      'rates_and_pricing', 'policies', 'events', 'awards',
      'meta_description', 'meta_data_description', 'open_graph_description',
      'alt_image_text', 'most_challenging_hole', 'scoring_version'
    ];
  }

  getJsonFields() {
    return [
      'form_category_averages', 'text_insight_averages', 'top_text_themes',
      'reviews_by_source', 'scores', 'detailed_scores_with_explanations',
      'strategic_analysis', 'weather_analysis', 'elevation_analysis'
    ];
  }

  getArrayFields() {
    return [
      'food_beverage_options'
    ];
  }
}
