/**
 * DATA RENDERER MODULE — PANEL-ONLY VERSION
 * Tabs are managed by TabManager. This file only renders panel contents.
 * - No self-managed tabs, no inline tab CSS/JS
 * - Keeps { showEmptyShell } for Overview without a selected course
 */

import {
  formatDisplayValue,
  getSourceBadge,
  isUrl,
  truncateUrl,
  formatFieldName,
  formatDate,
  getScoreColor,
  getRatingColor,
  getInsightColor
} from '../utils.js';

export class DataRenderer {
  constructor() {}

  // =========================
  // WRAPPER METHOD FOR APP.JS COMPATIBILITY
  // =========================
  /**
   * This method is called by app.js with (data, container) parameters
   * It wraps the internal rendering method to match the expected interface
   */
  renderPrimaryData(data, container) {
    // Handle both calling conventions
    if (container && container.nodeType === 1) {
      // Called with (data, DOMElement) - new style from app.js
      if (!data) {
        container.innerHTML = '<div class="status info">No primary data found for this course</div>';
        return;
      }

      const courseNumber = data.course_number;
      const teesAndParsData = {
        tees: data.tees || [],
        pars: data.pars || null
      };

      // Generate HTML using internal method
      const htmlContent = this.renderPrimaryDataInternal(data, courseNumber, teesAndParsData);

      // Set the HTML content to the container
      container.innerHTML = htmlContent;
    } else {
      // Called with old parameters - return HTML string
      return this.renderPrimaryDataInternal(data, container, arguments[2], arguments[3]);
    }
  }

  // =========================
  // INTERNAL PRIMARY DATA RENDERER
  // =========================
  /**
   * Internal method that generates HTML string for primary data
   */
  renderPrimaryDataInternal(data, courseNumber, teesAndParsData = null, options = {}) {
    const { showEmptyShell = false } = options;
    const d = data || {};

    // If there's truly no data and no shell requested, show gentle message
    if (!data && !showEmptyShell) {
      return '<div class="status info">No primary data found for this course</div>';
    }

    // Section field lists
    const basicInfo = ['course_name','city','state','county','zip_code','country','street_address','formatted_address','phone','website','email_address'];
    const courseDetails = ['architect','year_built_founded','status_type','total_holes','total_par','course_rating','slope_rating','total_length'];
    const courseCharacteristics = ['course_type','is_18_hole','is_9_hole','is_par_3_course','is_executive_course','has_ocean_views','has_scenic_views'];
    const amenities = ['has_driving_range','has_practice_green','has_short_game_area','has_clubhouse','has_pro_shop','has_locker_rooms','has_showers','has_beverage_cart','has_banquet_facilities'];
    const pricing = ['pricing_level','pricing_level_description','typical_18_hole_rate','pricing_information'];
    const urls = ['scorecard_url','about_url','membership_url','tee_time_url','rates_url','golfnow_url','golfpass_url','facebook_url','instagram_url','twitter_url','youtube_url','tiktok_url'];
    const location = ['latitude','longitude','place_id','street_number','route','photo_reference','google_maps_link'];
    const google = ['primary_type','user_rating_count','opening_hours'];

    const reviewsRatings = ['google_rating','google_review_count','total_reviews','overall_rating','recommend_percent','golfnow_reviews','golfpass_reviews'];
    const categoryRatings = ['conditions_rating','value_rating','friendliness_rating','pace_rating','amenities_rating','difficulty_rating','layout_rating'];
    const detailedScores = ['fairways_score','greens_score','bunkers_score','tee_boxes_score','shot_variety_score','signature_holes_score','water_ob_placement_score','overall_feel_scenery_score','green_complexity_score','staff_friendliness_score','green_fees_quality_score','replay_value_score','ease_walking_score','pace_play_score','availability_score'];

    const courseScores = ['course_score','course_score_percentage','max_score','course_conditions_score','course_conditions_max','course_layout_design_score','course_layout_design_max','amenities_score','amenities_max','player_experience_score','player_experience_max','scoring_version'];

    const vectorAttributes = ['beginner_friendly_score','ball_loss_risk_score','open_course_feeling','natural_character_score','water_course_score','ball_findability_score','tree_coverage_density','visual_tightness','natural_integration','water_prominence','course_openness','terrain_visual_complexity','elevation_feature_prominence','design_style_category','routing_style'];

    const strategicAnalysis = ['total_bunkers','left_biased_holes','right_biased_holes','course_bunker_bias','left_doglegs','right_doglegs','total_doglegs','dogleg_balance','overall_handedness_advantage','short_hitter_safety_pct','average_hitter_safety_pct','long_hitter_safety_pct'];

    const weatherAnalysis = ['weather_data_source','weather_years_analyzed','total_days_analyzed','avg_temp_c','golf_season_avg_temp_c','golf_season_length_months','golf_season_type','rainy_days_pct','heavy_rain_days_pct','avg_wind_kph','windy_days_pct','best_golf_month','best_golf_score','worst_golf_month','worst_golf_score'];

    const elevationAnalysis = ['total_elevation_change_m','average_elevation_change_m','max_single_hole_change_m','uphill_holes','downhill_holes','flat_holes','extreme_difficulty_holes','challenging_difficulty_holes','most_challenging_hole','analysis_timestamp'];

    const seoMarketing = ['slug','meta_title','meta_description','open_graph_title','open_graph_description','course_image_url','alt_image_text'];
    const certifications = ['acsp_certification','century_club','century_club_group','monarchs_in_the_rough'];

    return `
      <div class="primary-data-layout">
        ${teesAndParsData ? `
          <div class="full-width-sections">
            ${this.renderTeesData(teesAndParsData.tees || [], courseNumber, teesAndParsData.pars || null)}
          </div>` : ''
        }

        <div class="main-sections">
          ${this.renderSection('Basic Information','📍',basicInfo,d,courseNumber)}
          ${this.renderSection('Course Details','🏌️',courseDetails,d,courseNumber)}
          ${this.renderSection('Course Characteristics','🎯',courseCharacteristics,d,courseNumber)}
          ${this.renderSection('Amenities','🏆',amenities,d,courseNumber)}
          ${this.renderSection('Location Data','🌍',location,d,courseNumber)}
          ${this.renderSection('Google Places Data','🔍',google,d,courseNumber)}
          ${this.renderSection('Reviews & Ratings Overview','📊',reviewsRatings,d,courseNumber)}
          ${this.renderSection('Category Ratings','⭐',categoryRatings,d,courseNumber)}
          ${this.renderSection('Detailed Review Scores','🔍',detailedScores,d,courseNumber)}
          ${this.renderSection('Course Scores','📈',courseScores,d,courseNumber)}
          ${this.renderFullWidthSection('Vector Attributes','🎯',vectorAttributes,d,courseNumber)}
          ${this.renderSection('Strategic Analysis','⚔️',strategicAnalysis,d,courseNumber)}
          ${this.renderSection('Weather Analysis','🌤️',weatherAnalysis,d,courseNumber)}
          ${this.renderSection('Elevation Analysis','🏔️',elevationAnalysis,d,courseNumber)}
          ${this.renderSection('SEO & Marketing','📱',seoMarketing,d,courseNumber)}
          ${this.renderSection('Certifications','🏅',certifications,d,courseNumber)}
        </div>

        <div class="full-width-sections">
          ${this.renderFullWidthSection('Pricing Information','💰',pricing,d,courseNumber)}
          ${this.renderFullWidthSection('Important URLs','🔗',urls,d,courseNumber)}
        </div>
      </div>
    `;
  }

  // =========================
  // TEES DATA RENDERER
  // =========================
  renderTeesData(tees, courseNumber, pars = null) {
    if (!tees || tees.length === 0) {
      return '';
    }

    return `
      <div class="tees-pars-section">
        <h3>⛳ Tees & Pars Information</h3>
        <div class="tees-table-container">
          <table class="tees-pars-table">
            <thead>
              <tr>
                <th>Tee</th>
                <th>Rating</th>
                <th>Slope</th>
                <th>1</th>
                <th>2</th>
                <th>3</th>
                <th>4</th>
                <th>5</th>
                <th>6</th>
                <th>7</th>
                <th>8</th>
                <th>9</th>
                <th>Out</th>
                <th>10</th>
                <th>11</th>
                <th>12</th>
                <th>13</th>
                <th>14</th>
                <th>15</th>
                <th>16</th>
                <th>17</th>
                <th>18</th>
                <th>In</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              ${pars ? `
                <tr class="pars-row">
                  <td><strong>Par</strong></td>
                  <td>-</td>
                  <td>-</td>
                  ${[1,2,3,4,5,6,7,8,9].map(i => this._parCell(pars[`hole_${i}`])).join('')}
                  <td><strong>${pars.out_9 || '-'}</strong></td>
                  ${[10,11,12,13,14,15,16,17,18].map(i => this._parCell(pars[`hole_${i}`])).join('')}
                  <td><strong>${pars.in_9 || '-'}</strong></td>
                  <td><strong>${pars.total_par || '-'}</strong></td>
                </tr>` : ''
              }
              ${tees.map(tee => `
                <tr>
                  <td>${tee.tee_name || '-'}</td>
                  <td>${tee.rating || '-'}</td>
                  <td>${tee.slope || '-'}</td>
                  ${[1,2,3,4,5,6,7,8,9].map(i => `<td>${tee[`hole_${i}`] || '-'}</td>`).join('')}
                  <td><strong>${tee.out_9 || '-'}</strong></td>
                  ${[10,11,12,13,14,15,16,17,18].map(i => `<td>${tee[`hole_${i}`] || '-'}</td>`).join('')}
                  <td><strong>${tee.in_9 || '-'}</strong></td>
                  <td><strong>${tee.total_yardage || '-'}</strong></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  // =========================
  // SCORES PANEL
  // =========================
  renderScoresData(data) {
    if (!data) return this.renderEmpty('No course scores found for this course', '🎯');

    const scores = data.scores ? this._safeJson(data.scores) : {};
    const detailed = data.detailed_scores_with_explanations ? this._safeJson(data.detailed_scores_with_explanations) : {};

    const calcTotal = obj => (obj && typeof obj === 'object'
      ? Object.values(obj).reduce((s, n) => s + (Number(n) || 0), 0)
      : 0);

    const categoryTotals = {};
    let overallTotal = 0;
    Object.keys(scores).forEach(cat => {
      const t = calcTotal(scores[cat]);
      categoryTotals[cat] = t;
      overallTotal += t;
    });

    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>📊 Overall Score</h3>
          ${this.renderField('Total Score', data.total_score != null && data.max_score != null ? `${data.total_score}/${data.max_score}` : null)}
          ${this.renderField('Percentage', data.percentage != null ? `${data.percentage}%` : null)}
          ${this.renderField('Scoring Date', formatDate(data.scoring_date))}
          ${this.renderField('Data Source', data.data_source)}
        </div>
        <div class="data-card">
          <h3>📈 Category Totals</h3>
          ${this.renderField('Course Conditions', data.course_conditions_score != null && data.course_conditions_max != null ? `${data.course_conditions_score}/${data.course_conditions_max}` : null)}
          ${this.renderField('Layout & Design', data.course_layout_design_score != null && data.course_layout_design_max != null ? `${data.course_layout_design_score}/${data.course_layout_design_max}` : null)}
          ${this.renderField('Amenities', data.amenities_score != null && data.amenities_max != null ? `${data.amenities_score}/${data.amenities_max}` : null)}
          ${this.renderField('Player Experience', data.player_experience_score != null && data.player_experience_max != null ? `${data.player_experience_score}/${data.player_experience_max}` : null)}
        </div>
      </div>

      <div style="background: linear-gradient(135deg,#f0f9ff,#e0f2fe); border:1px solid #bae6fd; border-radius:15px; padding:25px; margin:25px 0;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
          <h3 style="color:#0369a1; margin:0;">🔍 Detailed Score Breakdown</h3>
          <div style="background:linear-gradient(135deg,#1e40af,#3b82f6); color:#fff; padding:8px 16px; border-radius:8px; font-weight:bold; font-size:1.1rem;">
            Total Score: ${overallTotal}
          </div>
        </div>
        ${Object.keys(scores).map(category => `
          <div style="margin-bottom:25px; border:1px solid rgba(14,165,233,0.2); border-radius:12px; padding:20px; background:rgba(255,255,255,0.5);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
              <h4 style="color:#0369a1; margin:0; font-size:1.1rem; font-weight:600;">${category}</h4>
              <div style="background:linear-gradient(135deg,#059669,#10b981); color:white; padding:6px 12px; border-radius:6px; font-weight:bold;">
                ${categoryTotals[category]} points
              </div>
            </div>
            ${Object.keys(scores[category] || {}).map(item => `
              <div style="display:flex; justify-content:space-between; align-items:center; padding:10px 0; font-size:14px; border-bottom:1px solid rgba(14,165,233,0.1);">
                <span style="color:#374151; flex:1;">${item}</span>
                <span style="color:#374151; font-weight:bold; background:#f1f5f9; padding:4px 8px; border-radius:4px; min-width:40px; text-align:center;">${scores[category][item]}</span>
              </div>
            `).join('')}
          </div>
        `).join('')}
      </div>

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Detailed Explanations JSON</summary>
        <div class="json-block">${this._jsonBlock(detailed)}</div>
      </details>
    `;
  }

  // =========================
  // VECTOR PANEL
  // =========================
  renderVectorData(data) {
    if (!data) return this.renderEmpty('No vector attributes found for this course', '🧬');

    const coreAttrs = [
      'ball_findability_score','tree_coverage_density','visual_tightness',
      'natural_integration','water_prominence','course_openness',
      'terrain_visual_complexity','elevation_feature_prominence'
    ];

    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>🎯 Core Attributes</h3>
          <div class="field-grid">
            ${coreAttrs.map(f => this.renderField(formatFieldName(f), data[f], null, null, null, getScoreColor(data[f]))).join('')}
          </div>
        </div>

        <div class="data-card">
          <h3>🏗️ Design Categories</h3>
          <div class="field-grid">
            ${this.renderField('Design Style', data.design_style_category)}
            ${this.renderField('Routing Style', data.routing_style)}
            ${this.renderField('Analysis Method', data.analysis_method)}
            ${this.renderField('Generation Date', formatDate(data.generation_timestamp))}
          </div>
        </div>

        <div class="data-card">
          <h3>📊 Analysis Details</h3>
          <div class="field-grid">
            ${this.renderField('Satellite Images', data.satellite_analysis_count)}
            ${this.renderField('Elevation Images', data.elevation_analysis_count)}
            ${this.renderField('Max Satellite Images', data.max_satellite_images)}
            ${this.renderField('Max Elevation Images', data.max_elevation_images)}
          </div>
        </div>
      </div>

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Full Vector Attributes JSON</summary>
        <div class="json-block">${this._jsonBlock(data.vector_attributes ? this._safeJson(data.vector_attributes) : {})}</div>
      </details>
    `;
  }

  // =========================
  // ANALYSIS PANEL
  // =========================
  renderAnalysisData(data) {
    if (!data) return this.renderEmpty('No comprehensive analysis found for this course', '📉');
    const d = data;

    const strategic = d.strategic_analysis ? this._safeJson(d.strategic_analysis) : {};
    const weather = d.weather_analysis ? this._safeJson(d.weather_analysis) : {};
    const elevation = d.elevation_analysis ? this._safeJson(d.elevation_analysis) : {};

    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>⛳ Strategic Summary</h3>
          <div class="field-grid">
            ${this.renderField('Total Holes', d.total_holes)}
            ${this.renderField('Total Doglegs', d.total_doglegs)}
            ${this.renderField('Left Doglegs', d.left_doglegs)}
            ${this.renderField('Right Doglegs', d.right_doglegs)}
            ${this.renderField('Dogleg Balance', d.dogleg_balance)}
            ${this.renderField('Handedness Advantage', d.overall_handedness_advantage)}
            ${this.renderField('Bunker Bias', d.course_bunker_bias)}
          </div>
        </div>

        <div class="data-card">
          <h3>🌤️ Weather Summary</h3>
          <div class="field-grid">
            ${this.renderField('Golf Season Length', d.golf_season_length_months != null ? `${d.golf_season_length_months} months` : null)}
            ${this.renderField('Season Type', d.golf_season_type)}
            ${this.renderField('Average Temp', d.avg_temp_c != null ? `${d.avg_temp_c}°C` : null)}
            ${this.renderField('Season Avg Temp', d.golf_season_avg_temp_c != null ? `${d.golf_season_avg_temp_c}°C` : null)}
            ${this.renderField('Rainy Days', d.rainy_days_pct != null ? `${d.rainy_days_pct}%` : null)}
            ${this.renderField('Windy Days', d.windy_days_pct != null ? `${d.windy_days_pct}%` : null)}
            ${this.renderField('Best Golf Month', d.best_golf_month)}
            ${this.renderField('Best Golf Score', d.best_golf_score)}
          </div>
        </div>

        <div class="data-card">
          <h3>🏔️ Elevation Summary</h3>
          <div class="field-grid">
            ${this.renderField('Total Elevation Change', d.total_elevation_change_m != null ? `${d.total_elevation_change_m}m` : null)}
            ${this.renderField('Average Change', d.average_elevation_change_m != null ? `${d.average_elevation_change_m}m` : null)}
            ${this.renderField('Max Single Hole', d.max_single_hole_change_m != null ? `${d.max_single_hole_change_m}m` : null)}
            ${this.renderField('Uphill Holes', d.uphill_holes)}
            ${this.renderField('Downhill Holes', d.downhill_holes)}
            ${this.renderField('Flat Holes', d.flat_holes)}
            ${this.renderField('Extreme Difficulty', d.extreme_difficulty_holes)}
            ${this.renderField('Most Challenging', d.most_challenging_hole ? `Hole ${d.most_challenging_hole}` : null)}
          </div>
        </div>

        <div class="data-card">
          <h3>🎯 Landing Zone Safety</h3>
          <div class="field-grid">
            ${this.renderField('Short Hitter Safety', d.short_hitter_safety_pct != null ? `${d.short_hitter_safety_pct}%` : null)}
            ${this.renderField('Average Hitter Safety', d.average_hitter_safety_pct != null ? `${d.average_hitter_safety_pct}%` : null)}
            ${this.renderField('Long Hitter Safety', d.long_hitter_safety_pct != null ? `${d.long_hitter_safety_pct}%` : null)}
            ${this.renderField('Analysis Date', formatDate(d.analysis_timestamp))}
            ${this.renderField('Data Source', d.weather_data_source)}
            ${this.renderField('Years Analyzed', d.weather_years_analyzed)}
          </div>
        </div>
      </div>

      ${this._renderWeatherMonths(weather)}
      ${this._renderElevationHoles(elevation)}

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Strategic Analysis JSON</summary>
        <div class="json-block">${this._jsonBlock(strategic)}</div>
      </details>
      <details style="margin-top:15px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Weather Analysis JSON</summary>
        <div class="json-block">${this._jsonBlock(weather)}</div>
      </details>
      <details style="margin-top:15px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Elevation Analysis JSON</summary>
        <div class="json-block">${this._jsonBlock(elevation)}</div>
      </details>
    `;
  }

  // =========================
  // REVIEWS PANEL
  // =========================
  renderReviewsData(data) {
    if (!data) return this.renderEmpty('No review data found for this course', '⭐');
    const d = data;

    const formAverages = d.form_category_averages ? this._safeJson(d.form_category_averages) : {};
    const textInsights = d.text_insight_averages ? this._safeJson(d.text_insight_averages) : {};
    const topThemes = d.top_text_themes ? this._safeJson(d.top_text_themes) : [];
    const reviewsBySource = d.reviews_by_source ? this._safeJson(d.reviews_by_source) : {};

    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>📊 Review Summary</h3>
          <div class="field-grid">
            ${this.renderField('Total Reviews', d.total_reviews)}
            ${this.renderField('Overall Rating', d.overall_rating != null ? `${(d.overall_rating || 0)}/5.0` : null, null, null, null, getRatingColor(d.overall_rating))}
            ${this.renderField('Recommend %', d.recommend_percent != null ? `${d.recommend_percent}%` : null)}
            ${this.renderField('GolfNow Reviews', d.golfnow_reviews)}
            ${this.renderField('GolfPass Reviews', d.golfpass_reviews)}
            ${this.renderField('Import Date', formatDate(d.import_date))}
          </div>
        </div>

        <div class="data-card">
          <h3>⭐ Form Category Ratings</h3>
          <div class="field-grid">
            ${this.renderField('Conditions', this._ratingText(d.conditions_rating), null, null, null, getRatingColor(d.conditions_rating))}
            ${this.renderField('Value', this._ratingText(d.value_rating), null, null, null, getRatingColor(d.value_rating))}
            ${this.renderField('Friendliness', this._ratingText(d.friendliness_rating), null, null, null, getRatingColor(d.friendliness_rating))}
            ${this.renderField('Pace', this._ratingText(d.pace_rating), null, null, null, getRatingColor(d.pace_rating))}
            ${this.renderField('Amenities', this._ratingText(d.amenities_rating), null, null, null, getRatingColor(d.amenities_rating))}
            ${this.renderField('Difficulty', this._ratingText(d.difficulty_rating), null, null, null, getRatingColor(d.difficulty_rating))}
            ${this.renderField('Layout', this._ratingText(d.layout_rating), null, null, null, getRatingColor(d.layout_rating))}
          </div>
        </div>

        <div class="data-card">
          <h3>💭 Text Insight Scores</h3>
          <div class="field-grid">
            ${this.renderField('Fairways Quality', this._pct(d.fairways_score), null, null, null, getInsightColor(d.fairways_score))}
            ${this.renderField('Greens Quality', this._pct(d.greens_score), null, null, null, getInsightColor(d.greens_score))}
            ${this.renderField('Overall Scenery', this._pct(d.overall_feel_scenery_score), null, null, null, getInsightColor(d.overall_feel_scenery_score))}
            ${this.renderField('Signature Holes', this._pct(d.signature_holes_score), null, null, null, getInsightColor(d.signature_holes_score))}
            ${this.renderField('Value for Money', this._pct(d.green_fees_quality_score), null, null, null, getInsightColor(d.green_fees_quality_score))}
            ${this.renderField('Pace of Play', this._pct(d.pace_play_score), null, null, null, getInsightColor(d.pace_play_score))}
            ${this.renderField('Replay Value', this._pct(d.replay_value_score), null, null, null, getInsightColor(d.replay_value_score))}
          </div>
        </div>
      </div>

      ${Array.isArray(topThemes) && topThemes.length ? `
        <div style="margin: 30px 0;">
          <h3 style="color:#2c3e50; font-size:1.3rem; margin-bottom:20px;">🏷️ Top Review Themes</h3>
          <div style="display:flex; flex-wrap:wrap; gap:12px;">
            ${topThemes.map(([theme, count]) => `
              <div style="background:linear-gradient(135deg, #e0f2fe, #b3e5fc); border:1px solid #81d4fa; border-radius:20px; padding:10px 15px; font-size:14px; font-weight:500;">
                <strong>${theme}</strong> <span style="color:#0277bd;">(${count})</span>
              </div>
            `).join('')}
          </div>
        </div>` : ''
      }

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Full Review Data JSON</summary>
        <div class="json-block">
          ${this._jsonBlock({ form_category_averages: formAverages, text_insight_averages: textInsights, top_text_themes: topThemes, reviews_by_source: reviewsBySource })}
        </div>
      </details>
    `;
  }

  // =========================
  // USGolf initial data PANEL
  // =========================
  renderUSGolfData(data) {
    if (!data) return this.renderEmpty('No initial course data found for this course', '🗂️');
    const d = data;

    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>📋 Basic Information</h3>
          <div class="field-grid">
            ${this.renderField('Course Number', d.course_number)}
            ${this.renderField('Course Name', d.course_name)}
            ${this.renderField('Status Type', d.status_type)}
            ${this.renderField('Email Address', d.email_address)}
            ${this.renderField('Website URL', d.website_url)}
            ${this.renderField('Process Status', d.process, null, null, d.course_number)}
          </div>
        </div>

        <div class="data-card">
          <h3>🏌️ Course Specifications</h3>
          <div class="field-grid">
            ${this.renderField('Total Holes', d.total_holes)}
            ${this.renderField('Total Par', d.total_par)}
            ${this.renderField('Course Rating', d.course_rating)}
            ${this.renderField('Slope Rating', d.slope_rating)}
            ${this.renderField('Total Length', d.total_length != null ? d.total_length + ' yards' : null)}
          </div>
        </div>

        <div class="data-card">
          <h3>🏗️ Course Details</h3>
          <div class="field-grid">
            ${this.renderField('Architect', d.architect)}
            ${this.renderField('Year Built/Founded', d.year_built_founded)}
            ${this.renderField('Created At', formatDate(d.created_at))}
            ${this.renderField('Updated At', formatDate(d.updated_at))}
          </div>
        </div>

        <div class="data-card">
          <h3>📍 Location & Contact</h3>
          <div class="field-grid">
            ${this.renderField('Street Address', d.street_address)}
            ${this.renderField('City', d.city)}
            ${this.renderField('State/Region', d.state_or_region)}
            ${this.renderField('County', d.county)}
            ${this.renderField('ZIP Code', d.zip_code)}
            ${this.renderField('Phone Number', d.phone_number)}
          </div>
        </div>

        <div class="data-card">
          <h3>🔧 System Information</h3>
          <div class="field-grid">
            ${this.renderField('Database ID', d.id)}
            ${this.renderField('Record Created', formatDate(d.created_at))}
            ${this.renderField('Last Modified', formatDate(d.updated_at))}
          </div>
        </div>
      </div>

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Raw Initial Course Data JSON</summary>
        <div class="json-block">${this._jsonBlock(d)}</div>
      </details>
    `;
  }

  /**
   * Google Places Data Panel Renderer
   */
   renderGooglePlacesData(data) {
     if (!data) return this.renderEmpty('No Google Places data found for this course', '📍');
     const d = data;

     return `
       <div class="data-grid">
         <div class="data-card">
           <h3>📍 Basic Information</h3>
           <div class="field-grid">
             ${this.renderField('Place ID', d.place_id)}
             ${this.renderField('Display Name', d.display_name)}
             ${this.renderField('Primary Type', d.primary_type)}
             ${this.renderField('Course Number', d.course_number)}
           </div>
         </div>

         <div class="data-card">
           <h3>🗺️ Location Details</h3>
           <div class="field-grid">
             ${this.renderField('Formatted Address', d.formatted_address)}
             ${this.renderField('Street Number', d.street_number)}
             ${this.renderField('Route', d.route)}
             ${this.renderField('Street Address', d.street_address)}
             ${this.renderField('City', d.city)}
             ${this.renderField('State', d.state)}
             ${this.renderField('County', d.county)}
             ${this.renderField('ZIP Code', d.zip_code)}
             ${this.renderField('Country', d.country)}
           </div>
         </div>

         <div class="data-card">
           <h3>📍 Coordinates</h3>
           <div class="field-grid">
             ${this.renderField('Latitude', d.latitude)}
             ${this.renderField('Longitude', d.longitude)}
             ${this.renderField('Google Maps Link', d.google_maps_link ? `<a href="${d.google_maps_link}" target="_blank" class="clickable-url">View on Google Maps</a>` : null)}
           </div>
         </div>

         <div class="data-card">
           <h3>📞 Contact & Business Info</h3>
           <div class="field-grid">
             ${this.renderField('Phone', d.phone)}
             ${this.renderField('Website', d.website ? `<a href="${d.website}" target="_blank" class="clickable-url">${d.website}</a>` : null)}
             ${this.renderField('Opening Hours', d.opening_hours)}
           </div>
         </div>

         <div class="data-card">
           <h3>⭐ Reviews & Media</h3>
           <div class="field-grid">
             ${this.renderField('User Rating Count', d.user_rating_count)}
             ${this.renderField('Photo Reference', d.photo_reference ? `<code style="font-size:11px; word-break:break-all;">${d.photo_reference.substring(0, 50)}...</code>` : null)}
           </div>
         </div>

         <div class="data-card">
           <h3>🕐 Timestamps</h3>
           <div class="field-grid">
             ${this.renderField('Created At', formatDate(d.created_at))}
             ${this.renderField('Updated At', formatDate(d.updated_at))}
           </div>
         </div>
       </div>

       <details style="margin-top:25px;">
         <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Raw Google Places Data JSON</summary>
         <div class="json-block">${this._jsonBlock(d)}</div>
       </details>
     `;
 }

  // Helper method for formatting opening hours
  _formatOpeningHours(hours) {
      if (typeof hours === 'string') {
          try {
              const parsed = JSON.parse(hours);
              if (parsed.weekday_text) {
                  return `<ul style="list-style:none; padding:0; margin:5px 0;">
                      ${parsed.weekday_text.map(day => `<li style="padding:3px 0;">${day}</li>`).join('')}
                  </ul>`;
              }
          } catch {
              return hours;
          }
      }
      return hours || 'Not available';
  }


  // =========================
  // WEBSITE SCRAPING PANEL
  // =========================
  renderScrapingData(data) {
    const d = data || {};
    return `
      <div class="data-grid">
        <div class="data-card">
          <h3>📋 General Information</h3>
          <div class="field-grid">
            ${this.renderField('Course Name', d.name)}
            ${this.renderField('Address', d.address)}
            ${this.renderField('Phone', d.phone)}
            ${this.renderField('Email', d.email)}
            ${this.renderField('Website', d.website)}
            ${this.renderField('Course Type', d.course_type)}
            ${d.pricing_level ? this.renderField('Pricing Tier', `<span class="pricing-tier pricing-tier-${d.pricing_level}">Level ${d.pricing_level}</span>`) : this.renderField('Pricing Tier', null)}
            ${this.renderField('Typical Rate', d.typical_18_hole_rate)}
          </div>
        </div>

        <div class="data-card">
          <h3>🏌️ Course Details</h3>
          <div class="field-grid">
            ${this.renderField('18-Hole Course', d.is_18_hole === true ? 'Yes' : d.is_18_hole === false ? 'No' : null)}
            ${this.renderField('9-Hole Course', d.is_9_hole === true ? 'Yes' : d.is_9_hole === false ? 'No' : null)}
            ${this.renderField('Par-3 Course', d.is_par_3_course === true ? 'Yes' : d.is_par_3_course === false ? 'No' : null)}
            ${this.renderField('Executive Course', d.is_executive_course === true ? 'Yes' : d.is_executive_course === false ? 'No' : null)}
            ${this.renderField('Ocean Views', d.has_ocean_views === true ? 'Yes' : d.has_ocean_views === false ? 'No' : null)}
            ${this.renderField('Scenic Views', d.has_scenic_views === true ? 'Yes' : d.has_scenic_views === false ? 'No' : null)}
            ${this.renderField('Architect', d.architect)}
            ${this.renderField('Year Built', d.year_built)}
          </div>
        </div>

        <div class="data-card">
          <h3>💰 Pricing & Rates</h3>
          <div class="field-grid">
            ${this.renderField('Pricing Level', d.pricing_level)}
            ${this.renderField('Pricing Description', d.pricing_level_description)}
            ${this.renderField('Typical 18-Hole Rate', d.typical_18_hole_rate)}
          </div>
        </div>

        <div class="data-card">
          <h3>🔗 Important URLs</h3>
          <div class="field-grid">
            ${this.renderField('Scorecard URL', d.scorecard_url)}
            ${this.renderField('About URL', d.about_url)}
            ${this.renderField('Membership URL', d.membership_url)}
            ${this.renderField('Tee Time URL', d.tee_time_url)}
            ${this.renderField('Rates URL', d.rates_url)}
          </div>
        </div>

        <div class="data-card">
          <h3>🏆 Recognition & History</h3>
          <div class="field-grid">
            ${this.renderArrayField('Awards', d.recognitions)}
            ${this.renderArrayField('Rankings', d.rankings)}
            ${this.renderArrayField('Certifications', d.certifications)}
            ${this.renderArrayField('Course History', d.course_history_general)}
            ${this.renderArrayField('Notable Events', d.notable_events)}
            ${this.renderArrayField('Design Features', d.design_features)}
          </div>
        </div>

        <div class="data-card">
          <h3>📊 Data Quality</h3>
          <div class="field-grid">
            ${this.renderField('Pages Crawled', d.pages_crawled)}
            ${this.renderField('ML Extractions', d.ml_extractions)}
            ${this.renderField('Regex Extractions', d.regex_extractions)}
            ${this.renderField('Last Updated', formatDate(d.last_updated))}
            ${this.renderField('Import Date', formatDate(d.import_date))}
            ${this.renderField('Spider Version', d.spider_version || 'N/A')}
          </div>
        </div>
      </div>

      ${d.signature_holes ? `
        <div style="margin: 30px 0;">
          <h3 style="color:#2c3e50; margin-bottom:20px;">🎯 Signature Holes</h3>
          <div style="background: linear-gradient(135deg, #f0f9ff, #e0f2fe); border: 1px solid #bae6fd; border-radius: 15px; padding: 25px;">
            <p style="margin:0; color:#0369a1; line-height:1.7;">${d.signature_holes}</p>
          </div>
        </div>` : ''
      }

      ${(d.food_beverage_options || d.food_beverage_description) ? `
        <div style="margin: 30px 0;">
          <h3 style="color:#2c3e50; margin-bottom:20px;">🍽️ Food & Beverage Options</h3>
          <div style="background: linear-gradient(135deg, #f0fdf4, #dcfce7); border: 1px solid #bbf7d0; border-radius: 15px; padding: 25px;">
            ${d.food_beverage_options ? `<p style="margin:0 0 15px 0; color:#166534; line-height:1.7;"><strong>Options:</strong> ${d.food_beverage_options}</p>` : ''}
            ${d.food_beverage_description ? `<p style="margin:0; color:#166534; line-height:1.7;"><strong>Description:</strong> ${d.food_beverage_description}</p>` : ''}
          </div>
        </div>` : ''
      }

      ${d.course_policies ? `
        <div style="margin: 30px 0;">
          <h3 style="color:#2c3e50; margin-bottom:20px;">📋 Course Policies</h3>
          <div style="background: linear-gradient(135deg, #fef3c7, #fde68a); border: 1px solid #fbbf24; border-radius: 15px; padding: 25px;">
            <p style="margin:0; color:#92400e; line-height:1.7;">${d.course_policies}</p>
          </div>
        </div>` : ''
      }

      ${d.pricing_information ? `
        <div style="margin: 30px 0;">
          <h3 style="color:#2c3e50; margin-bottom:20px;">💰 Detailed Pricing Information</h3>
          <div style="background: linear-gradient(135deg, #f0fdf4, #dcfce7); border: 1px solid #bbf7d0; border-radius: 15px; padding: 25px;">
            <pre style="white-space: pre-wrap; font-family: inherit; margin: 0; color:#166534; line-height:1.7;">${d.pricing_information}</pre>
          </div>
        </div>` : ''
      }

      <details style="margin-top:25px;">
        <summary style="cursor:pointer; font-weight:600; color:#2c3e50; font-size:1.1rem;">View Raw Course Information JSON</summary>
        <div class="json-block">${this._jsonBlock(d)}</div>
      </details>
    `;
  }

  // =========================
  // JSONB / ADVANCED SECTION
  // =========================
  renderJsonbSection(title, icon, fields, data, courseNumber) {
    if (!fields || fields.length === 0) return '';
    const d = data || {};
    return `
      <div class="jsonb-section">
        <h3>${icon} ${title}</h3>
        ${fields.map(field => {
          const value = d[field];
          const source = d[`${field}_source`];
          const hasData = value && value !== '{}' && value !== 'null' && value !== '';

          return `
            <div class="jsonb-field">
              <div class="jsonb-field-header">
                <strong>
                  ${formatFieldName(field)}: ${getSourceBadge(source || 'Unknown')}
                </strong>
                <button class="edit-btn" data-field="${field}" data-value="${encodeURIComponent(value || '')}" data-course="${courseNumber}" title="Edit ${formatFieldName(field)}">✏️</button>
              </div>
              <div class="jsonb-field-content">
                ${hasData
                  ? `<details><summary>View ${formatFieldName(field)} Data</summary><div class="json-display">${this._formatJsonForDisplay(value)}</div></details>`
                  : '<span style="color:#6b7280; font-style:italic;">No data available</span>'}
              </div>
            </div>
          `;
        }).join('')}
      </div>
    `;
  }

  // =========================
  // SHARED SECTION RENDERERS
  // =========================
  renderSection(title, icon, fields, data, courseNumber, className = 'data-card') {
    const d = data || {};
    return `
      <div class="${className}">
        <h3>${icon} ${title}</h3>
        <div class="compact-field-grid">
          ${fields.map(field => this._compactField(field, d[field], d[field + '_source'], d[field + '_updated_at'], courseNumber)).join('')}
        </div>
      </div>
    `;
  }

  renderFullWidthSection(title, icon, fields, data, courseNumber) {
    const d = data || {};
    return `
      <div class="full-width-card">
        <h3>${icon} ${title}</h3>
        <div class="compact-field-grid">
          ${fields.map(field => this._compactField(field, d[field], d[field + '_source'], d[field + '_updated_at'], courseNumber)).join('')}
        </div>
      </div>
    `;
  }

  renderTextSection(title, icon, fields, data, courseNumber) {
    const d = data || {};
    return `
      <div class="text-section">
        <h3>${icon} ${title}</h3>
        ${fields.map(field => `
          <div class="text-field">
            <div class="text-field-header">
              <strong>
                ${formatFieldName(field)}:
                ${getSourceBadge(d[`${field}_source`] || 'Unknown')}
              </strong>
              <button class="edit-btn" data-field="${field}" data-value="${encodeURIComponent(d[field] || '')}" data-course="${courseNumber}">✏️</button>
            </div>
            <div class="text-field-content">${d[field] || '<span style="color:#6b7280; font-style:italic;">No data available</span>'}</div>
          </div>
        `).join('')}
      </div>
    `;
  }

  renderArrayField(label, array) {
    if (!array || !Array.isArray(array) || array.length === 0) {
      return label ? this._field(label, 'None') : '';
    }
    const content = `
      <ul style="list-style:none; padding:0; margin:5px 0;">
        ${array.map(item => `<li style="padding:8px 0; border-bottom:1px solid #f1f3f4; color:#495057;">${item}</li>`).join('')}
      </ul>
    `;
    return `
      <div class="field-row">
        <div class="field-content">
          <div class="field-label">${label}</div>
          <div class="field-value">${content}</div>
        </div>
      </div>
    `;
  }

  // =========================
  // GENERIC UI STATES
  // =========================
  renderLoading(message = 'Loading data...') {
    return `
      <div style="display:flex; align-items:center; justify-content:center; padding:40px; color:#6b7280;">
        <div style="margin-right:15px;">
          <div style="width:20px; height:20px; border:2px solid #e5e7eb; border-top:2px solid #3b82f6; border-radius:50%; animation:spin 1s linear infinite;"></div>
        </div>
        <span style="font-weight:500;">${message}</span>
      </div>
      <style>
        @keyframes spin { 0% {transform:rotate(0)} 100% {transform:rotate(360deg)} }
      </style>
    `;
  }

  renderError(message, details = null) {
    return `
      <div style="background: linear-gradient(135deg,#fef2f2,#fecaca); border:1px solid #fca5a5; border-radius:12px; padding:20px; margin:20px 0;">
        <div style="display:flex; align-items:center; margin-bottom:15px;">
          <span style="font-size:1.5rem; margin-right:12px;">⚠️</span>
          <h3 style="margin:0; color:#dc2626; font-size:1.2rem;">Error</h3>
        </div>
        <p style="margin:0 0 10px 0; color:#7f1d1d; font-weight:500;">${message}</p>
        ${details ? `
          <details style="margin-top:15px;">
            <summary style="cursor:pointer; color:#991b1b; font-weight:500;">View Details</summary>
            <pre style="background:rgba(255,255,255,0.8); padding:15px; border-radius:8px; margin-top:10px; color:#7f1d1d; font-size:13px; overflow-x:auto;">${details}</pre>
          </details>` : ''
        }
      </div>
    `;
  }

  renderEmpty(message = 'No data available', icon = '📭') {
    return `
      <div style="text-align:center; padding:60px 20px; color:#6b7280;">
        <div style="font-size:4rem; margin-bottom:20px; opacity:.5;">${icon}</div>
        <h3 style="margin:0 0 10px 0; font-size:1.5rem; font-weight:600; color:#374151;">${message}</h3>
        <p style="margin:0; font-size:1rem; opacity:.8;">There's nothing to display at the moment.</p>
      </div>
    `;
  }

  // =========================
  // INTERNAL HELPERS
  // =========================
  renderField(fieldName, value, source, updatedAt, courseNumber, colorOverride) {
    return this._field(fieldName, value, source, updatedAt, courseNumber, colorOverride);
  }

  _field(fieldName, value, source, updatedAt, courseNumber, colorOverride) {
    const displayValue = formatDisplayValue(value);
    const valueClass = value != null ? '' : 'null';
    const sourceInfo = source ? getSourceBadge(source) : '';

    let manualIndicator = '';
    if (source === 'manual') {
      const updateDate = updatedAt ? new Date(updatedAt).toLocaleDateString() : '';
      manualIndicator = `<span class="manual-indicator" title="Manually updated ${updateDate}">MANUAL</span>`;
    }

    const finalDisplayValue = isUrl(value)
      ? `<a href="${value}" target="_blank" class="clickable-url">${truncateUrl(value)}</a>`
      : displayValue;

    const editButton = courseNumber
      ? `<button class="edit-btn" data-field="${fieldName}" data-value="${encodeURIComponent(value || '')}" data-course="${courseNumber}" title="Edit ${formatFieldName(fieldName)}">✏️</button>`
      : '';

    const colorStyle = colorOverride ? `style="color:${colorOverride};"` : '';

    return `
      <div class="field-row">
        <div class="field-content">
          <div class="field-label">
            ${formatFieldName(fieldName)}
            ${sourceInfo}
            ${manualIndicator}
          </div>
          <div class="field-value ${valueClass}" ${colorStyle}>${finalDisplayValue}</div>
        </div>
        <div class="field-actions">${editButton}</div>
      </div>
    `;
  }

  _compactField(fieldName, value, source, updatedAt, courseNumber) {
    const displayValue = formatDisplayValue(value);
    const valueClass = value != null ? '' : 'null';
    const sourceInfo = source ? getSourceBadge(source) : '';

    let manualIndicator = '';
    if (source === 'manual') {
      const updateDate = updatedAt ? new Date(updatedAt).toLocaleDateString() : '';
      manualIndicator = `<span class="manual-indicator" title="Manually updated ${updateDate}">M</span>`;
    }

    const finalDisplayValue = isUrl(value)
      ? `<a href="${value}" target="_blank" class="clickable-url" title="${value}">${value}</a>`
      : displayValue;

    const editButton = courseNumber
      ? `<button class="edit-btn" data-field="${fieldName}" data-value="${encodeURIComponent(value || '')}" data-course="${courseNumber}" title="Edit ${formatFieldName(fieldName)}">✏️</button>`
      : '';

    return `
      <div class="compact-field-row">
        <div class="compact-field-header">
          <div class="compact-field-label">
            ${formatFieldName(fieldName)}
            ${sourceInfo}
            ${manualIndicator}
          </div>
          <div class="compact-field-actions">${editButton}</div>
        </div>
        <div class="compact-field-value ${valueClass}">${finalDisplayValue}</div>
      </div>
    `;
  }

  _parCell(par) {
    let cls = '';
    if (par == 3) cls = 'par-3-cell';
    else if (par == 4) cls = 'par-4-cell';
    else if (par == 5) cls = 'par-5-cell';
    return `<td class="${cls}" style="font-weight:600;">${par || '-'}</td>`;
  }

  _ratingText(v) { return v != null ? `${v}/5.0` : null; }
  _pct(v) { if (v == null || isNaN(v)) return null; return `${(Number(v) * 100).toFixed(0)}%`; }

  _safeJson(value) {
    try { return typeof value === 'string' ? JSON.parse(value) : (value || {}); }
    catch { return {}; }
  }

  _jsonBlock(obj) { return JSON.stringify(obj || {}, 2, 2); } // pretty

  _formatJsonForDisplay(value) {
    if (!value) return 'null';
    try {
      const parsed = typeof value === 'string' ? JSON.parse(value) : value;
      return JSON.stringify(parsed, null, 2);
    } catch {
      return String(value);
    }
  }

  // =========================
  // MINOR VISUAL HELPERS
  // =========================
  _renderWeatherMonths(weather) {
    if (!weather || !weather.playable_months) return '';
    const best = (weather.best_golf_month || '').toLowerCase();
    const worst = (weather.worst_golf_month || '').toLowerCase();

    return `
      <div style="margin:30px 0;">
        <h3 style="color:#2c3e50;margin-bottom:25px;font-size:1.3rem;">🌤️ Golf Season Months</h3>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:15px;">
          ${weather.playable_months.map(m => {
            const ml = (m || '').toLowerCase();
            const bg = ml === best ? 'linear-gradient(135deg,#dcfce7,#bbf7d0)'
                     : ml === worst ? 'linear-gradient(135deg,#fef2f2,#fecaca)'
                     : 'linear-gradient(135deg,#fef3c7,#fde68a)';
            const bd = ml === best ? '#22c55e' : ml === worst ? '#ef4444' : '#fbbf24';
            const col = ml === best ? '#166534' : ml === worst ? '#dc2626' : '#92400e';
            const tag = ml === best ? '🏆 Best' : ml === worst ? '⚠️ Worst' : '✅ Playable';
            return `
              <div style="background:${bg};border:1px solid ${bd};border-radius:12px;padding:15px;text-align:center;font-size:13px;font-weight:500;">
                <strong style="display:block;margin-bottom:5px;">${m}</strong>
                <span style="font-size:11px;color:${col};">${tag}</span>
              </div>`;
          }).join('')}
        </div>
      </div>
    `;
  }

  _renderElevationHoles(elevation) {
    if (!elevation || !elevation.hole_elevation_analysis) return '';
    const holes = elevation.hole_elevation_analysis;

    return `
      <div style="margin:30px 0;">
        <h3 style="color:#2c3e50;margin-bottom:25px;font-size:1.3rem;">🏔️ Hole Elevation Analysis</h3>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:15px;">
          ${Object.keys(holes).map(num => {
            const h = holes[num];
            const bg = h.difficulty_rating === 'extreme' ? 'linear-gradient(135deg,#fef2f2,#fecaca)'
                     : h.difficulty_rating === 'challenging' ? 'linear-gradient(135deg,#fef3c7,#fde68a)'
                     : 'linear-gradient(135deg,#f8fafc,#f1f5f9)';
            const bd = h.difficulty_rating === 'extreme' ? '#dc2626'
                     : h.difficulty_rating === 'challenging' ? '#f59e0b'
                     : '#e2e8f0';
            const stripe = h.hole_type === 'uphill' ? '#16a34a'
                        : h.hole_type === 'downhill' ? '#3b82f6'
                        : '#6b7280';
            const change = (h.net_elevation_change_m > 0 ? '+' : '') + h.net_elevation_change_m + 'm';
            return `
              <div style="background:${bg};border:1px solid ${bd};border-left:4px solid ${stripe};border-radius:12px;padding:15px;text-align:center;font-size:12px;">
                <strong style="display:block;margin-bottom:8px;font-size:14px;">Hole ${num}</strong>
                <div style="font-size:16px;font-weight:bold;margin-bottom:5px;color:#2c3e50;">${change}</div>
                <small style="color:#6b7280;text-transform:capitalize;">${h.difficulty_rating || ''}</small>
              </div>`;
          }).join('')}
        </div>
      </div>
    `;
  }

  // =========================
  // SOCIAL (used by text panels)
  // =========================
  renderSocial(data) {
    const d = data || {};
    const socials = [
      { key: 'facebook_url', label: 'Facebook', icon: '📘' },
      { key: 'instagram_url', label: 'Instagram', icon: '📷' },
      { key: 'twitter_url', label: 'Twitter', icon: '🐦' },
      { key: 'youtube_url', label: 'YouTube', icon: '📺' },
      { key: 'tiktok_url', label: 'TikTok', icon: '🎵' }
    ];
    const hasAny = socials.some(s => d[s.key]);
    if (!hasAny) return '';
    return `
      <div style="margin:30px 0;">
        <h3 style="color:#2c3e50;margin-bottom:25px;font-size:1.3rem;">📱 Social Media</h3>
        <div style="display:flex;flex-wrap:wrap;gap:15px;">
          ${socials.map(s => d[s.key] ? `
            <a href="${d[s.key]}" target="_blank" style="text-decoration:none;">
              <div style="background:linear-gradient(135deg,#e0f2fe,#b3e5fc);border:1px solid #81d4fa;border-radius:12px;padding:15px 20px;display:flex;align-items:center;transition:all .3s ease;cursor:pointer;">
                <span style="font-size:1.2rem;margin-right:10px;">${s.icon}</span>
                <span style="color:#0277bd;font-weight:600;">${s.label}</span>
              </div>
            </a>` : ''
          ).join('')}
        </div>
      </div>
    `;
  }
}
