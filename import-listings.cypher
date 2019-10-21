# Import Milan listings
USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM "file://listings.csv" AS row FIELDTERMINATOR ','
WITH row WHERE row.id IS NOT NULL
MERGE (l:Listing {listing_id: row.id})
ON CREATE SET l.name                        = row.name,
              l.latitude                    = toFloat(row.latitude),
              l.longitude                   = toFloat(row.longitude),
              l.reviews_per_month           = toFloat(row.reviews_per_month),
              l.cancellation_policy         = row.cancellation_policy,
              l.instant_bookable            = CASE WHEN row.instant_bookable = "t" THEN true ELSE false END,
              l.review_scores_value         = toInt(row.review_scores_value),
              l.review_scores_location      = toInt(row.review_scores_location),
              l.review_scores_communication = toInt(row.review_scores_communication),
              l.review_scores_checkin       = toInt(row.review_scores_checking),
              l.review_scores_cleanliness   = toInt(row.review_scores_cleanliness),
              l.reivew_scores_accuracy      = toInt(row.review_scores_accuracy),
              l.review_scores_rating        = toInt(row.review_scores_rating),
              l.availability_365            = toInt(row.availability_365),
              l.availability_90             = toInt(row.availability_90),
              l.availability_60             = toInt(row.availability_60),
              l.availability_30             = toInt(row.availability_30),
              l.price                       = toFloat(substring(row.price, 1)),
              l.cleaning_fee                = toFloat(substring(row.cleaning_free, 1)),
              l.security_deposit            = toFloat(substring(row.security_deposit, 1)),
              l.monthly_price               = toFloat(substring(row.monthly_price, 1)),
              l.weekly_price                = toFloat(substring(row.weekly_price, 1)),
              l.square_feet                 = toInt(row.square_feet),
              l.bed_type                    = row.bed_type,
              l.beds                        = toInt(row.beds),
              l.bedrooms                    = toInt(row.bedrooms),
              l.bathrooms                   = toFloat(row.bathrooms),
              l.accommodates                = toInt(row.accommodates),
              l.room_type                   = row.room_type,
              l.property_type               = row.property_type
ON MATCH SET l.count = coalesce(l.count, 0) + 1

