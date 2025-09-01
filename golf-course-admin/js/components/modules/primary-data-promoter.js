/**
 * PRIMARY DATA PROMOTER MODULE
 * Handles promotion of data from source tables to primary_data
 */

export class PrimaryDataPromoter {
    constructor(pipelineManager) {
        this.pipelineManager = pipelineManager;
        this.database = null;
    }

    setDatabase(database) {
        this.database = database;
    }

    async promoteAllMissingCourses() {
        if (!this.database || !this.database.client) {
            console.error('Database not connected');
            throw new Error('Database not connected');
        }

        try {
            // Get all courses from initial_course_upload
            const { data: courses, error } = await this.database.client
                .from('initial_course_upload')
                .select('course_number');

            if (error) throw error;

            console.log(`Found ${courses.length} courses to promote`);
            let created = 0;
            let updated = 0;
            let failed = 0;

            for (const course of courses) {
                try {
                    // Check if primary_data record exists
                    const { data: existing } = await this.database.client
                        .from('primary_data')
                        .select('course_number')
                        .eq('course_number', course.course_number)
                        .single();

                    if (!existing) {
                        // Create primary_data record from initial_course_upload
                        const { data: initialData } = await this.database.client
                            .from('initial_course_upload')
                            .select('*')
                            .eq('course_number', course.course_number)
                            .single();

                        if (initialData) {
                            const now = new Date().toISOString();
                            const primaryRecord = {
                                course_number: initialData.course_number,
                                course_name: initialData.course_name,
                                course_name_source: 'usgolf_data',
                                course_name_updated_at: now,
                                street_address: initialData.street_address,
                                street_address_source: 'usgolf_data',
                                street_address_updated_at: now,
                                city: initialData.city,
                                city_source: 'usgolf_data',
                                city_updated_at: now,
                                county: initialData.county,
                                county_source: 'usgolf_data',
                                county_updated_at: now,
                                state: initialData.state_or_region,
                                state_source: 'usgolf_data',
                                state_updated_at: now,
                                zip_code: initialData.zip_code,
                                zip_code_source: 'usgolf_data',
                                zip_code_updated_at: now,
                                phone: initialData.phone_number,
                                phone_source: 'usgolf_data',
                                phone_updated_at: now,
                                website: initialData.website_url,
                                website_source: 'usgolf_data',
                                website_updated_at: now,
                                email_address: initialData.email_address,
                                email_address_source: 'usgolf_data',
                                email_address_updated_at: now,
                                architect: initialData.architect,
                                architect_source: 'usgolf_data',
                                architect_updated_at: now,
                                year_built_founded: initialData.year_built_founded,
                                year_built_founded_source: 'usgolf_data',
                                year_built_founded_updated_at: now,
                                status_type: initialData.status_type,
                                status_type_source: 'usgolf_data',
                                status_type_updated_at: now,
                                total_par: initialData.total_par,
                                total_par_source: 'usgolf_data',
                                total_par_updated_at: now,
                                total_holes: initialData.total_holes,
                                total_holes_source: 'usgolf_data',
                                total_holes_updated_at: now,
                                course_rating: initialData.course_rating,
                                course_rating_source: 'usgolf_data',
                                course_rating_updated_at: now,
                                slope_rating: initialData.slope_rating,
                                slope_rating_source: 'usgolf_data',
                                slope_rating_updated_at: now,
                                total_length: initialData.total_length,
                                total_length_source: 'usgolf_data',
                                total_length_updated_at: now,
                                created_at: now
                            };

                            await this.database.client
                                .from('primary_data')
                                .insert(primaryRecord);

                            console.log(`✅ Created primary record for ${course.course_number}`);
                            created++;
                        }
                    }

                    // Now update with data from other tables in priority order
                    await this.updateFromGooglePlaces(course.course_number);
                    await this.updateFromCourseScrapingData(course.course_number);
                    await this.updateFromReviewUrls(course.course_number);
                    updated++;

                } catch (err) {
                    console.error(`Failed for ${course.course_number}:`, err);
                    failed++;
                }
            }

            console.log(`✅ Promotion complete! Created: ${created}, Updated: ${updated}, Failed: ${failed}`);
            return { created, updated, failed };

        } catch (error) {
            console.error('Promotion failed:', error);
            throw error;
        }
    }

    async updateFromGooglePlaces(courseNumber) {
        try {
            const { data } = await this.database.client
                .from('google_places_data')
                .select('*')
                .eq('course_number', courseNumber)
                .single();

            if (data) {
                const now = new Date().toISOString();
                const updates = {};

                // Map all Google Places fields to primary_data fields
                // Basic fields
                if (data.display_name) {
                    updates.course_name = data.display_name;
                    updates.course_name_source = 'google_places_data';
                    updates.course_name_updated_at = now;
                }

                // Address components
                if (data.formatted_address) {
                    updates.formatted_address = data.formatted_address;
                    updates.formatted_address_source = 'google_places_data';
                    updates.formatted_address_updated_at = now;
                }
                if (data.street_address) {
                    updates.street_address = data.street_address;
                    updates.street_address_source = 'google_places_data';
                    updates.street_address_updated_at = now;
                }
                if (data.street_number) {
                    updates.street_number = data.street_number;
                    updates.street_number_source = 'google_places_data';
                    updates.street_number_updated_at = now;
                }
                if (data.route) {
                    updates.route = data.route;
                    updates.route_source = 'google_places_data';
                    updates.route_updated_at = now;
                }
                if (data.city) {
                    updates.city = data.city;
                    updates.city_source = 'google_places_data';
                    updates.city_updated_at = now;
                }
                if (data.state) {
                    updates.state = data.state;
                    updates.state_source = 'google_places_data';
                    updates.state_updated_at = now;
                }
                if (data.zip_code) {
                    updates.zip_code = data.zip_code;
                    updates.zip_code_source = 'google_places_data';
                    updates.zip_code_updated_at = now;
                }
                if (data.county) {
                    updates.county = data.county;
                    updates.county_source = 'google_places_data';
                    updates.county_updated_at = now;
                }
                if (data.country) {
                    updates.country = data.country;
                    updates.country_source = 'google_places_data';
                    updates.country_updated_at = now;
                }

                // Contact info
                if (data.phone) {
                    updates.phone = data.phone;
                    updates.phone_source = 'google_places_data';
                    updates.phone_updated_at = now;
                }
                if (data.website) {
                    updates.website = data.website;
                    updates.website_source = 'google_places_data';
                    updates.website_updated_at = now;
                }

                // Location data
                if (data.latitude !== null && data.latitude !== undefined) {
                    updates.latitude = data.latitude;
                    updates.latitude_source = 'google_places_data';
                    updates.latitude_updated_at = now;
                }
                if (data.longitude !== null && data.longitude !== undefined) {
                    updates.longitude = data.longitude;
                    updates.longitude_source = 'google_places_data';
                    updates.longitude_updated_at = now;
                }

                // Google-specific fields
                if (data.place_id) {
                    updates.place_id = data.place_id;
                    updates.place_id_source = 'google_places_data';
                    updates.place_id_updated_at = now;
                }
                if (data.google_maps_link) {
                    updates.google_maps_link = data.google_maps_link;
                    updates.google_maps_link_source = 'google_places_data';
                    updates.google_maps_link_updated_at = now;
                }
                if (data.photo_reference) {
                    updates.photo_reference = data.photo_reference;
                    updates.photo_reference_source = 'google_places_data';
                    updates.photo_reference_updated_at = now;
                }

                // Business info
                if (data.primary_type) {
                    updates.primary_type = data.primary_type;
                    updates.primary_type_source = 'google_places_data';
                    updates.primary_type_updated_at = now;
                }
                if (data.opening_hours) {
                    updates.opening_hours = data.opening_hours;
                    updates.opening_hours_source = 'google_places_data';
                    updates.opening_hours_updated_at = now;
                }

                // Ratings and reviews
                if (data.rating !== null && data.rating !== undefined) {
                    updates.google_rating = data.rating;
                    updates.google_rating_source = 'google_places_data';
                    updates.google_rating_updated_at = now;
                }
                if (data.user_rating_count !== null && data.user_rating_count !== undefined) {
                    updates.user_rating_count = data.user_rating_count;
                    updates.user_rating_count_source = 'google_places_data';
                    updates.user_rating_count_updated_at = now;

                    // Also update google_review_count if it maps to the same field
                    updates.google_review_count = data.user_rating_count;
                    updates.google_review_count_source = 'google_places_data';
                    updates.google_review_count_updated_at = now;
                }

                // Always update the main updated_at timestamp
                updates.updated_at = now;

                if (Object.keys(updates).length > 1) {
                    const { error } = await this.database.client
                        .from('primary_data')
                        .update(updates)
                        .eq('course_number', courseNumber);

                    if (error) {
                        console.error(`Error updating primary_data from Google Places for ${courseNumber}:`, error);
                    } else {
                        console.log(`✅ Updated ${Object.keys(updates).length - 1} fields from Google Places for ${courseNumber}`);
                    }
                }
            }
        } catch (err) {
            // Silently handle if no google places data exists
            if (err.code !== 'PGRST116') {
                console.warn(`Error updating from Google Places for ${courseNumber}:`, err);
            }
        }
    }
    
    async updateFromCourseScrapingData(courseNumber) {
        try {
            const { data } = await this.database.client
                .from('course_scraping_data')
                .select('*')
                .eq('course_number', courseNumber)
                .single();

            if (data) {
                const now = new Date().toISOString();
                const updates = {};

                if (data.course_description) {
                    updates.course_description = data.course_description;
                    updates.course_description_source = 'course_scraping_data';
                    updates.course_description_updated_at = now;
                }
                if (data.pricing_information) {
                    updates.pricing_information = data.pricing_information;
                    updates.pricing_information_source = 'course_scraping_data';
                    updates.pricing_information_updated_at = now;
                }
                if (data.has_driving_range !== null && data.has_driving_range !== undefined) {
                    updates.has_driving_range = data.has_driving_range;
                    updates.has_driving_range_source = 'course_scraping_data';
                    updates.has_driving_range_updated_at = now;
                }
                if (data.has_practice_green !== null && data.has_practice_green !== undefined) {
                    updates.has_practice_green = data.has_practice_green;
                    updates.has_practice_green_source = 'course_scraping_data';
                    updates.has_practice_green_updated_at = now;
                }
                if (data.has_pro_shop !== null && data.has_pro_shop !== undefined) {
                    updates.has_pro_shop = data.has_pro_shop;
                    updates.has_pro_shop_source = 'course_scraping_data';
                    updates.has_pro_shop_updated_at = now;
                }
                if (data.has_clubhouse !== null && data.has_clubhouse !== undefined) {
                    updates.has_clubhouse = data.has_clubhouse;
                    updates.has_clubhouse_source = 'course_scraping_data';
                    updates.has_clubhouse_updated_at = now;
                }
                if (data.food_beverage_options) {
                    updates.food_beverage_options = data.food_beverage_options;
                    updates.food_beverage_options_source = 'course_scraping_data';
                    updates.food_beverage_options_updated_at = now;
                }
                if (data.food_beverage_description) {
                    updates.food_beverage_description = data.food_beverage_description;
                    updates.food_beverage_description_source = 'course_scraping_data';
                    updates.food_beverage_description_updated_at = now;
                }
                if (data.has_short_game_area !== null && data.has_short_game_area !== undefined) {
                    updates.has_short_game_area = data.has_short_game_area;
                    updates.has_short_game_area_source = 'course_scraping_data';
                    updates.has_short_game_area_updated_at = now;
                }
                if (data.has_locker_rooms !== null && data.has_locker_rooms !== undefined) {
                    updates.has_locker_rooms = data.has_locker_rooms;
                    updates.has_locker_rooms_source = 'course_scraping_data';
                    updates.has_locker_rooms_updated_at = now;
                }
                if (data.has_showers !== null && data.has_showers !== undefined) {
                    updates.has_showers = data.has_showers;
                    updates.has_showers_source = 'course_scraping_data';
                    updates.has_showers_updated_at = now;
                }

                // Always update the main updated_at timestamp
                updates.updated_at = now;

                if (Object.keys(updates).length > 1) {
                    await this.database.client
                        .from('primary_data')
                        .update(updates)
                        .eq('course_number', courseNumber);
                }
            }
        } catch (err) {
            // Silently handle if no scraping data exists
            if (err.code !== 'PGRST116') {
                console.warn(`Error updating from Course Scraping for ${courseNumber}:`, err);
            }
        }
    }

    async updateFromReviewUrls(courseNumber) {
        try {
            const { data } = await this.database.client
                .from('review_urls')
                .select('*')
                .eq('course_number', courseNumber)
                .single();

            if (data) {
                const now = new Date().toISOString();
                const updates = {};

                if (data.golf_now_url) {
                    updates.golfnow_url = data.golf_now_url;
                    updates.golfnow_url_source = 'review_urls';
                    updates.golfnow_url_updated_at = now;
                }
                if (data.golf_pass_url) {
                    updates.golfpass_url = data.golf_pass_url;
                    updates.golfpass_url_source = 'review_urls';
                    updates.golfpass_url_updated_at = now;
                }

                // Always update the main updated_at timestamp
                updates.updated_at = now;

                if (Object.keys(updates).length > 1) {
                    await this.database.client
                        .from('primary_data')
                        .update(updates)
                        .eq('course_number', courseNumber);
                }
            }
        } catch (err) {
            // Silently handle if no review URLs exist
            if (err.code !== 'PGRST116') {
                console.warn(`Error updating from Review URLs for ${courseNumber}:`, err);
            }
        }
    }

    async promoteSingleCourse(courseNumber) {
        if (!this.database || !this.database.client) {
            throw new Error('Database not connected');
        }

        try {
            // Check if primary_data record exists
            const { data: existing } = await this.database.client
                .from('primary_data')
                .select('course_number')
                .eq('course_number', courseNumber)
                .single();

            if (!existing) {
                // Create from initial_course_upload
                const { data: initialData } = await this.database.client
                    .from('initial_course_upload')
                    .select('*')
                    .eq('course_number', courseNumber)
                    .single();

                if (!initialData) {
                    throw new Error(`No initial course data found for ${courseNumber}`);
                }

                const now = new Date().toISOString();
                const primaryRecord = {
                    course_number: initialData.course_number,
                    course_name: initialData.course_name,
                    course_name_source: 'usgolf_data',
                    course_name_updated_at: now,
                    street_address: initialData.street_address,
                    street_address_source: 'usgolf_data',
                    street_address_updated_at: now,
                    city: initialData.city,
                    city_source: 'usgolf_data',
                    city_updated_at: now,
                    county: initialData.county,
                    county_source: 'usgolf_data',
                    county_updated_at: now,
                    state: initialData.state_or_region,
                    state_source: 'usgolf_data',
                    state_updated_at: now,
                    zip_code: initialData.zip_code,
                    zip_code_source: 'usgolf_data',
                    zip_code_updated_at: now,
                    phone: initialData.phone_number,
                    phone_source: 'usgolf_data',
                    phone_updated_at: now,
                    website: initialData.website_url,
                    website_source: 'usgolf_data',
                    website_updated_at: now,
                    email_address: initialData.email_address,
                    email_address_source: 'usgolf_data',
                    email_address_updated_at: now,
                    architect: initialData.architect,
                    architect_source: 'usgolf_data',
                    architect_updated_at: now,
                    year_built_founded: initialData.year_built_founded,
                    year_built_founded_source: 'usgolf_data',
                    year_built_founded_updated_at: now,
                    status_type: initialData.status_type,
                    status_type_source: 'usgolf_data',
                    status_type_updated_at: now,
                    total_par: initialData.total_par,
                    total_par_source: 'usgolf_data',
                    total_par_updated_at: now,
                    total_holes: initialData.total_holes,
                    total_holes_source: 'usgolf_data',
                    total_holes_updated_at: now,
                    course_rating: initialData.course_rating,
                    course_rating_source: 'usgolf_data',
                    course_rating_updated_at: now,
                    slope_rating: initialData.slope_rating,
                    slope_rating_source: 'usgolf_data',
                    slope_rating_updated_at: now,
                    total_length: initialData.total_length,
                    total_length_source: 'usgolf_data',
                    total_length_updated_at: now,
                    created_at: now
                };

                await this.database.client
                    .from('primary_data')
                    .insert(primaryRecord);

                console.log(`✅ Created primary record for ${courseNumber}`);
            }

            // Update with data from other tables
            await this.updateFromGooglePlaces(courseNumber);
            await this.updateFromCourseScrapingData(courseNumber);
            await this.updateFromReviewUrls(courseNumber);

            console.log(`✅ Promoted data for ${courseNumber}`);
            return true;

        } catch (error) {
            console.error(`Failed to promote ${courseNumber}:`, error);
            throw error;
        }
    }
}
