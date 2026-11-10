// Licensed under the Apache License, Version 2.0.
// Derived from Prometheus model/histogram/generic.go and util/jsonutil/marshal.go.

use crate::{FloatHistogram, CUSTOM_SCHEMA};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bucket {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: f64,
    pub index: i32,
}

impl Bucket {
    /// Returns the estimated fraction of this bucket at or below `value`.
    ///
    /// The caller must ensure that `value` is strictly inside finite bucket
    /// bounds. Custom and zero buckets use linear interpolation; exponential
    /// buckets use logarithmic interpolation.
    pub fn fraction_below(self, value: f64, linear: bool) -> f64 {
        if linear {
            return (value - self.lower) / (self.upper - self.lower);
        }
        let log_lower = self.lower.abs().log2();
        let log_upper = self.upper.abs().log2();
        let log_value = value.abs().log2();
        if value > 0.0 {
            (log_value - log_lower) / (log_upper - log_lower)
        } else {
            1.0 - (log_value - log_upper) / (log_lower - log_upper)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JsonBucket {
    pub boundaries: u8,
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
}

impl FloatHistogram {
    pub fn positive_buckets_iter(&self) -> impl Iterator<Item = Bucket> + '_ {
        bucket_indices(&self.positive_spans, &self.positive_buckets)
            .map(|(index, count)| bucket(self.schema, &self.custom_values, index, count, true))
    }

    pub fn negative_buckets_iter(&self) -> impl Iterator<Item = Bucket> + '_ {
        bucket_indices(&self.negative_spans, &self.negative_buckets)
            .map(|(index, count)| bucket(self.schema, &self.custom_values, index, count, false))
    }

    pub fn all_buckets(&self) -> AllBuckets<'_> {
        AllBuckets {
            histogram: self,
            cursor: BucketCursor::forward(self),
        }
    }

    pub fn all_buckets_rev(&self) -> AllBuckets<'_> {
        AllBuckets {
            histogram: self,
            cursor: BucketCursor::reverse(self),
        }
    }

    /// Buckets in the exact order and boundary-code form used by Prometheus JSON responses.
    pub fn json_buckets(&self) -> impl Iterator<Item = JsonBucket> + '_ {
        self.all_buckets()
            .filter(|bucket| bucket.count != 0.0)
            .map(|bucket| JsonBucket {
                boundaries: match (bucket.lower_inclusive, bucket.upper_inclusive) {
                    (false, true) => 0,
                    (true, false) => 1,
                    (false, false) => 2,
                    (true, true) => 3,
                },
                lower: bucket.lower,
                upper: bucket.upper,
                count: bucket.count,
            })
    }
}

/// Allocation-free traversal state that can be retained separately from a histogram.
/// Every `next` call must use the same histogram, with its layout and values unchanged.
#[derive(Clone, Debug)]
pub struct BucketCursor {
    negative: SideCursor,
    positive: SideCursor,
    stage: Stage,
    reverse: bool,
}

impl BucketCursor {
    pub fn forward(histogram: &FloatHistogram) -> Self {
        Self {
            negative: SideCursor::reverse(&histogram.negative_spans),
            positive: SideCursor::forward(),
            stage: Stage::Negative,
            reverse: false,
        }
    }

    pub fn reverse(histogram: &FloatHistogram) -> Self {
        Self {
            negative: SideCursor::forward(),
            positive: SideCursor::reverse(&histogram.positive_spans),
            stage: Stage::Positive,
            reverse: true,
        }
    }

    pub fn next(&mut self, histogram: &FloatHistogram) -> Option<Bucket> {
        loop {
            match self.stage {
                Stage::Negative => {
                    if let Some((index, count)) = self
                        .negative
                        .next(&histogram.negative_spans, &histogram.negative_buckets)
                    {
                        return Some(adjust_for_zero(
                            bucket(
                                histogram.schema,
                                &histogram.custom_values,
                                index,
                                count,
                                false,
                            ),
                            histogram.zero_threshold,
                        ));
                    }
                    self.stage = if self.reverse {
                        Stage::Done
                    } else {
                        Stage::ZeroToPositive
                    };
                }
                Stage::Positive => {
                    if let Some((index, count)) = self
                        .positive
                        .next(&histogram.positive_spans, &histogram.positive_buckets)
                    {
                        return Some(adjust_for_zero(
                            bucket(
                                histogram.schema,
                                &histogram.custom_values,
                                index,
                                count,
                                true,
                            ),
                            histogram.zero_threshold,
                        ));
                    }
                    self.stage = if self.reverse {
                        Stage::ZeroToNegative
                    } else {
                        Stage::Done
                    };
                }
                Stage::ZeroToPositive => {
                    self.stage = Stage::Positive;
                    if histogram.zero_count > 0.0 {
                        return Some(zero_bucket(histogram));
                    }
                }
                Stage::ZeroToNegative => {
                    self.stage = Stage::Negative;
                    if histogram.zero_count > 0.0 {
                        return Some(zero_bucket(histogram));
                    }
                }
                Stage::Done => return None,
            }
        }
    }
}

/// Borrowing iterator over all histogram buckets.
pub struct AllBuckets<'a> {
    histogram: &'a FloatHistogram,
    cursor: BucketCursor,
}

impl Iterator for AllBuckets<'_> {
    type Item = Bucket;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next(self.histogram)
    }
}

#[derive(Clone, Copy, Debug)]
enum Stage {
    Negative,
    ZeroToPositive,
    Positive,
    ZeroToNegative,
    Done,
}

#[derive(Clone, Debug)]
struct SideCursor {
    reverse: bool,
    span: usize,
    position_in_span: u32,
    bucket: usize,
    next_index: i64,
    entered_span: bool,
    done: bool,
}

impl SideCursor {
    fn forward() -> Self {
        Self {
            reverse: false,
            span: 0,
            position_in_span: 0,
            bucket: 0,
            next_index: 0,
            entered_span: false,
            done: false,
        }
    }

    fn reverse(spans: &[crate::Span]) -> Self {
        let next_index = spans
            .iter()
            .map(|span| i64::from(span.offset) + i64::from(span.length))
            .sum();
        Self {
            reverse: true,
            span: spans.len().saturating_sub(1),
            position_in_span: spans.last().map_or(0, |span| span.length),
            bucket: spans.iter().map(|span| span.length as usize).sum(),
            next_index,
            entered_span: true,
            done: spans.is_empty(),
        }
    }

    fn next(&mut self, spans: &[crate::Span], buckets: &[f64]) -> Option<(i32, f64)> {
        if self.done {
            return None;
        }
        if self.reverse {
            self.next_reverse(spans, buckets)
        } else {
            self.next_forward(spans, buckets)
        }
    }

    fn next_forward(&mut self, spans: &[crate::Span], buckets: &[f64]) -> Option<(i32, f64)> {
        loop {
            let Some(span) = spans.get(self.span) else {
                self.done = true;
                return None;
            };
            if !self.entered_span {
                self.next_index += i64::from(span.offset);
                self.entered_span = true;
            }
            if self.position_in_span < span.length {
                let index =
                    i32::try_from(self.next_index).expect("validated histogram bucket index");
                let count = *buckets
                    .get(self.bucket)
                    .expect("validated histogram bucket count");
                self.next_index += 1;
                self.position_in_span += 1;
                self.bucket += 1;
                return Some((index, count));
            }
            self.span += 1;
            self.position_in_span = 0;
            self.entered_span = false;
        }
    }

    fn next_reverse(&mut self, spans: &[crate::Span], buckets: &[f64]) -> Option<(i32, f64)> {
        loop {
            if self.position_in_span > 0 {
                self.next_index -= 1;
                self.position_in_span -= 1;
                self.bucket -= 1;
                let index =
                    i32::try_from(self.next_index).expect("validated histogram bucket index");
                let count = *buckets
                    .get(self.bucket)
                    .expect("validated histogram bucket count");
                return Some((index, count));
            }
            if self.span == 0 {
                self.done = true;
                return None;
            }
            self.next_index -= i64::from(spans[self.span].offset);
            self.span -= 1;
            self.position_in_span = spans[self.span].length;
        }
    }
}

fn zero_bucket(histogram: &FloatHistogram) -> Bucket {
    Bucket {
        lower: -histogram.zero_threshold,
        upper: histogram.zero_threshold,
        lower_inclusive: true,
        upper_inclusive: true,
        count: histogram.zero_count,
        index: 0,
    }
}

fn adjust_for_zero(mut bucket: Bucket, zero_threshold: f64) -> Bucket {
    if bucket.upper < 0.0 && bucket.upper > -zero_threshold {
        bucket.upper = -zero_threshold;
    } else if bucket.lower > 0.0 && bucket.lower < zero_threshold {
        bucket.lower = zero_threshold;
    }
    bucket
}

fn bucket_indices<'a>(
    spans: &'a [crate::Span],
    buckets: &'a [f64],
) -> impl Iterator<Item = (i32, f64)> + 'a {
    let mut index = 0i64;
    let mut bucket = 0usize;
    spans.iter().flat_map(move |span| {
        index += i64::from(span.offset);
        let start = index;
        index += i64::from(span.length);
        let start_bucket = bucket;
        bucket += span.length as usize;
        (0..span.length as usize).map(move |offset| {
            let index =
                i32::try_from(start + offset as i64).expect("validated histogram bucket index");
            (index, buckets[start_bucket + offset])
        })
    })
}

// Keep the small custom-bound path in the iterator. The exponential boundary
// calculation must not make every custom bucket pay an out-of-line struct return.
#[inline(always)]
pub(crate) fn bucket(
    schema: i32,
    custom_values: &[f64],
    index: i32,
    count: f64,
    positive: bool,
) -> Bucket {
    let upper = bound(i64::from(index), schema, custom_values);
    let lower = bound(i64::from(index) - 1, schema, custom_values);
    if positive {
        Bucket {
            lower,
            upper,
            lower_inclusive: schema == CUSTOM_SCHEMA && index == 0,
            upper_inclusive: true,
            count,
            index,
        }
    } else {
        Bucket {
            lower: -upper,
            upper: -lower,
            lower_inclusive: true,
            upper_inclusive: false,
            count,
            index,
        }
    }
}

fn bound(index: i64, schema: i32, custom_values: &[f64]) -> f64 {
    if schema == CUSTOM_SCHEMA {
        return match index {
            -1 => f64::NEG_INFINITY,
            index if usize::try_from(index).ok() == Some(custom_values.len()) => f64::INFINITY,
            _ => custom_values[usize::try_from(index).expect("validated custom bucket index")],
        };
    }
    exponential_bound_wide(index, schema)
}

pub(crate) fn exponential_bound(index: i32, schema: i32) -> f64 {
    exponential_bound_wide(i64::from(index), schema)
}

fn exponential_bound_wide(index: i64, schema: i32) -> f64 {
    if schema < 0 {
        let exponent = index << (-schema as u32);
        if exponent == 1024 {
            return f64::MAX;
        }
        return power_of_two(exponent);
    }
    let fractional_index = index & i64::from((1 << schema) - 1);
    let fraction = SCHEMA_8_BOUNDS[fractional_index as usize * (1 << (8 - schema))];
    let exponent = (index >> schema) + 1;
    if fraction == 0.5 && exponent == 1025 {
        return f64::MAX;
    }
    // Prometheus getBound uses math.Ldexp: 2^1024 itself overflows, but
    // fraction * 2^1024 is still finite for every table entry below 1.
    if exponent == 1024 {
        (fraction * 2.0) * power_of_two(1023)
    } else {
        fraction * power_of_two(exponent)
    }
}

fn power_of_two(exponent: i64) -> f64 {
    match exponent {
        1024.. => f64::INFINITY,
        -1022..=1023 => f64::from_bits(((exponent + 1023) as u64) << 52),
        // powi with a negative exponent can overflow its intermediate positive
        // power before taking the reciprocal. Go's math.Ldexp keeps subnormals.
        -1074..=-1023 => f64::from_bits(1 << (exponent + 1074)),
        _ => 0.0,
    }
}

// Every lower-resolution table is a strided subset of Prometheus's schema 8 table.
const SCHEMA_8_BOUNDS: [f64; 256] = [
    0.5,
    0.5013556375251013,
    0.5027149505564014,
    0.5040779490592088,
    0.5054446430258502,
    0.5068150424757447,
    0.5081891574554764,
    0.509566998038869,
    0.5109485743270583,
    0.5123338964485679,
    0.5137229745593818,
    0.5151158188430205,
    0.5165124395106142,
    0.5179128468009786,
    0.5193170509806894,
    0.520725062344158,
    0.5221368912137069,
    0.5235525479396449,
    0.5249720429003435,
    0.526395386502313,
    0.5278225891802786,
    0.5292536613972564,
    0.5306886136446309,
    0.5321274564422321,
    0.5335702003384117,
    0.5350168559101208,
    0.5364674337629877,
    0.5379219445313954,
    0.5393803988785598,
    0.5408428074966075,
    0.5423091811066545,
    0.5437795304588847,
    0.5452538663326288,
    0.5467321995364429,
    0.5482145409081883,
    0.549700901315111,
    0.5511912916539204,
    0.5526857228508706,
    0.5541842058618393,
    0.5556867516724088,
    0.5571933712979462,
    0.5587040757836845,
    0.5602188762048033,
    0.5617377836665098,
    0.5632608093041209,
    0.564787964283144,
    0.5663192597993595,
    0.5678547070789026,
    0.5693943173783458,
    0.5709381019847808,
    0.572486072215902,
    0.5740382394200894,
    0.5755946149764913,
    0.5771552102951081,
    0.5787200368168754,
    0.5802891060137493,
    0.5818624293887887,
    0.5834400184762408,
    0.585021884841625,
    0.5866080400818185,
    0.5881984958251406,
    0.5897932637314379,
    0.5913923554921704,
    0.5929957828304968,
    0.5946035575013605,
    0.5962156912915756,
    0.5978321960199137,
    0.5994530835371903,
    0.6010783657263515,
    0.6027080545025619,
    0.6043421618132907,
    0.6059806996384005,
    0.6076236799902344,
    0.6092711149137041,
    0.6109230164863786,
    0.6125793968185725,
    0.6142402680534349,
    0.6159056423670379,
    0.6175755319684665,
    0.6192499490999082,
    0.620928906036742,
    0.622612415087629,
    0.6243004885946023,
    0.6259931389331581,
    0.6276903785123455,
    0.6293922197748583,
    0.6310986751971253,
    0.6328097572894031,
    0.6345254785958666,
    0.6362458516947014,
    0.637970889198196,
    0.6397006037528346,
    0.6414350080393891,
    0.6431741147730128,
    0.6449179367033329,
    0.6466664866145447,
    0.6484197773255048,
    0.6501778216898253,
    0.6519406325959679,
    0.6537082229673385,
    0.6554806057623822,
    0.6572577939746774,
    0.659039800633032,
    0.6608266388015788,
    0.6626183215798706,
    0.6644148621029772,
    0.6662162735415805,
    0.6680225691020727,
    0.6698337620266515,
    0.6716498655934177,
    0.6734708931164728,
    0.6752968579460171,
    0.6771277734684463,
    0.6789636531064505,
    0.6808045103191123,
    0.6826503586020058,
    0.6845012114872953,
    0.6863570825438342,
    0.688217985377265,
    0.690083933630119,
    0.6919549409819159,
    0.6938310211492645,
    0.6957121878859629,
    0.6975984549830999,
    0.6994898362691555,
    0.7013863456101023,
    0.7032879969095076,
    0.7051948041086352,
    0.7071067811865475,
    0.7090239421602076,
    0.7109463010845827,
    0.7128738720527471,
    0.7148066691959849,
    0.7167447066838943,
    0.718687998724491,
    0.7206365595643126,
    0.7225904034885232,
    0.7245495448210174,
    0.7265139979245261,
    0.7284837772007218,
    0.7304588970903234,
    0.7324393720732029,
    0.7344252166684908,
    0.7364164454346837,
    0.7384130729697496,
    0.7404151139112358,
    0.7424225829363761,
    0.7444354947621984,
    0.7464538641456323,
    0.7484777058836176,
    0.7505070348132126,
    0.7525418658117031,
    0.7545822137967112,
    0.7566280937263048,
    0.7586795205991071,
    0.7607365094544071,
    0.762799075372269,
    0.7648672334736434,
    0.7669409989204777,
    0.7690203869158282,
    0.7711054127039704,
    0.7731960915705107,
    0.7752924388424999,
    0.7773944698885442,
    0.7795022001189185,
    0.7816156449856788,
    0.7837348199827764,
    0.7858597406461707,
    0.7879904225539431,
    0.7901268813264122,
    0.7922691326262467,
    0.7944171921585818,
    0.7965710756711334,
    0.7987307989543135,
    0.8008963778413465,
    0.8030678282083853,
    0.805245165974627,
    0.8074284071024302,
    0.8096175675974316,
    0.8118126635086642,
    0.8140137109286738,
    0.8162207259936375,
    0.8184337248834821,
    0.820652723822003,
    0.8228777390769823,
    0.8251087869603088,
    0.8273458838280969,
    0.8295890460808079,
    0.8318382901633681,
    0.8340936325652911,
    0.8363550898207981,
    0.8386226785089391,
    0.8408964152537144,
    0.8431763167241966,
    0.8454623996346523,
    0.8477546807446661,
    0.8500531768592616,
    0.8523579048290255,
    0.8546688815502312,
    0.8569861239649629,
    0.8593096490612387,
    0.8616394738731368,
    0.8639756154809185,
    0.8663180910111553,
    0.8686669176368529,
    0.871022112577578,
    0.8733836930995842,
    0.8757516765159389,
    0.8781260801866495,
    0.8805069215187917,
    0.8828942179666361,
    0.8852879870317771,
    0.8876882462632604,
    0.890095013257712,
    0.8925083056594671,
    0.8949281411607002,
    0.8973545375015533,
    0.8997875124702672,
    0.9022270839033115,
    0.9046732696855155,
    0.9071260877501991,
    0.909585556079304,
    0.9120516927035263,
    0.9145245157024483,
    0.9170040432046711,
    0.9194902933879467,
    0.9219832844793128,
    0.9244830347552253,
    0.9269895625416926,
    0.92950288621441,
    0.9320230241988943,
    0.9345499949706191,
    0.9370838170551498,
    0.93962450902828,
    0.9421720895161669,
    0.9447265771954693,
    0.9472879907934827,
    0.9498563490882775,
    0.9524316709088368,
    0.9550139751351947,
    0.9576032806985735,
    0.9601996065815236,
    0.9628029718180622,
    0.9654133954938133,
    0.9680308967461471,
    0.9706554947643201,
    0.9732872087896164,
    0.9759260581154889,
    0.9785720620876999,
    0.9812252401044634,
    0.9838856116165875,
    0.9865531961276168,
    0.9892280131939752,
    0.9919100824251095,
    0.9945994234836328,
    0.9972960560854698,
];
