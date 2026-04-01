#!/usr/bin/env perl

use strict;
use warnings;
use utf8;

use IO::Handle ();
use JSON::PP ();
use Scalar::Util qw(looks_like_number);

use ProductOpener::Config qw(:all);
use ProductOpener::DataQualityFood qw(is_european_product);
use ProductOpener::Products qw(analyze_and_enrich_product_data normalize_product_data);
use ProductOpener::Tags qw(get_all_tags_having_property get_inherited_property_from_categories_tags);

my $json = JSON::PP->new->utf8(1);
my $result_contract_kind = "openfoodfacts_data_quality.legacy_backend_reference_result";
my $result_contract_version = 1;

die "Usage: off_runtime.pl\n" if @ARGV;

my $input_fh = *STDIN;
my $output_fh = *STDOUT;
$output_fh->autoflush(1);

sub _snapshot_number {
	my ($value) = @_;
	return undef if not defined $value;
	return undef if ref($value);
	return looks_like_number($value) ? sprintf("%.17g", $value) : $value;
}

sub _snapshot_array {
	my ($value) = @_;
	return [] if ref($value) ne 'ARRAY';
	return $value;
}

sub _snapshot_hash {
	my ($value) = @_;
	return {} if ref($value) ne 'HASH';
	return $value;
}

sub _snapshot_packaging {
	my ($packaging_ref) = @_;
	return undef if ref($packaging_ref) ne 'HASH';
	return {
		number => _snapshot_number($packaging_ref->{number}),
		shape => $packaging_ref->{shape},
		material => $packaging_ref->{material},
	};
}

sub _snapshot_packagings {
	my ($value) = @_;
	return [] if ref($value) ne 'ARRAY';

	my @packagings = ();
	foreach my $packaging_ref (@{$value}) {
		my $packaging = _snapshot_packaging($packaging_ref);
		push @packagings, $packaging if defined $packaging;
	}
	return \@packagings;
}

sub _snapshot_ingredient {
	my ($ingredient_ref) = @_;
	return undef if ref($ingredient_ref) ne 'HASH';

	return {
		id => $ingredient_ref->{id},
		vegan => $ingredient_ref->{vegan},
		vegetarian => $ingredient_ref->{vegetarian},
		ingredients => _snapshot_ingredients($ingredient_ref->{ingredients}),
	};
}

sub _snapshot_ingredients {
	my ($value) = @_;
	return [] if ref($value) ne 'ARRAY';

	my @ingredients = ();
	foreach my $ingredient_ref (@{$value}) {
		my $ingredient = _snapshot_ingredient($ingredient_ref);
		push @ingredients, $ingredient if defined $ingredient;
	}
	return \@ingredients;
}

sub _snapshot_nutrient {
	my ($nutrient_ref) = @_;
	return undef if ref($nutrient_ref) ne 'HASH';

	return {
		value => _snapshot_number($nutrient_ref->{value}),
		unit => $nutrient_ref->{unit},
		value_computed => _snapshot_number($nutrient_ref->{value_computed}),
	};
}

sub _snapshot_nutrients {
	my ($value) = @_;
	return {} if ref($value) ne 'HASH';

	my %nutrients = ();
	foreach my $nutrient_id (keys %{$value}) {
		my $nutrient = _snapshot_nutrient($value->{$nutrient_id});
		next if not defined $nutrient;
		$nutrients{$nutrient_id} = $nutrient;
	}
	return \%nutrients;
}

sub _snapshot_nutrition_set {
	my ($set_ref) = @_;
	return undef if ref($set_ref) ne 'HASH';

	return {
		source => $set_ref->{source},
		preparation => $set_ref->{preparation},
		per => $set_ref->{per},
		nutrients => _snapshot_nutrients($set_ref->{nutrients}),
	};
}

sub _snapshot_nutrition_sets {
	my ($value) = @_;
	return [] if ref($value) ne 'ARRAY';

	my @sets = ();
	foreach my $set_ref (@{$value}) {
		my $set = _snapshot_nutrition_set($set_ref);
		push @sets, $set if defined $set;
	}
	return \@sets;
}

sub _snapshot_nutrition {
	my ($value) = @_;
	my $nutrition_ref = _snapshot_hash($value);

	return {
		input_sets => _snapshot_nutrition_sets($nutrition_ref->{input_sets}),
		aggregated_set => {
			nutrients => _snapshot_nutrients(_snapshot_hash($nutrition_ref->{aggregated_set})->{nutrients}),
		},
	};
}

sub _enriched_product_snapshot_for {
	my ($product_ref) = @_;
	return {
		code => $product_ref->{code},
		lc => $product_ref->{lc},
		lang => $product_ref->{lang},
		created_t => _snapshot_number($product_ref->{created_t}),
		packagings => _snapshot_packagings($product_ref->{packagings}),
		product_name => $product_ref->{product_name},
		quantity => $product_ref->{quantity},
		product_quantity => _snapshot_number($product_ref->{product_quantity}),
		serving_size => $product_ref->{serving_size},
		serving_quantity => _snapshot_number($product_ref->{serving_quantity}),
		brands => $product_ref->{brands},
		categories => $product_ref->{categories},
		labels => $product_ref->{labels},
		emb_codes => $product_ref->{emb_codes},
		ingredients_text => $product_ref->{ingredients_text},
		ingredients => _snapshot_ingredients($product_ref->{ingredients}),
		ingredients_percent_analysis => _snapshot_number($product_ref->{ingredients_percent_analysis}),
		ingredients_with_specified_percent_n => _snapshot_number($product_ref->{ingredients_with_specified_percent_n}),
		ingredients_with_unspecified_percent_n => _snapshot_number($product_ref->{ingredients_with_unspecified_percent_n}),
		ingredients_with_specified_percent_sum => _snapshot_number($product_ref->{ingredients_with_specified_percent_sum}),
		ingredients_with_unspecified_percent_sum => _snapshot_number($product_ref->{ingredients_with_unspecified_percent_sum}),
		nutriscore_grade => $product_ref->{nutriscore_grade},
		nutriscore_grade_producer => $product_ref->{nutriscore_grade_producer},
		nutriscore_score => _snapshot_number($product_ref->{nutriscore_score}),
		categories_tags => _snapshot_array($product_ref->{categories_tags}),
		labels_tags => _snapshot_array($product_ref->{labels_tags}),
		countries_tags => _snapshot_array($product_ref->{countries_tags}),
		food_groups_tags => _snapshot_array($product_ref->{food_groups_tags}),
	};
}

sub _enriched_flags_snapshot_for {
	my ($helper_state_ref) = @_;

	return {
		is_european_product => $helper_state_ref->{is_european_product},
		has_animal_origin_category => $helper_state_ref->{has_animal_origin_category},
		ignore_energy_calculated_error => $helper_state_ref->{ignore_energy_calculated_error},
	};
}

sub _enriched_category_props_snapshot_for {
	my ($helper_state_ref) = @_;

	return {
		minimum_number_of_ingredients => _snapshot_number($helper_state_ref->{minimum_number_of_ingredients}),
	};
}

sub _enriched_snapshot_for {
	my ($product_ref, $helper_state_ref) = @_;

	return {
		product => _enriched_product_snapshot_for($product_ref),
		flags => _enriched_flags_snapshot_for($helper_state_ref),
		category_props => _enriched_category_props_snapshot_for($helper_state_ref),
		nutrition => _snapshot_nutrition($product_ref->{nutrition}),
	};
}

sub _legacy_check_tags_for {
	my ($product_ref) = @_;

	return {
		bug => _snapshot_array($product_ref->{data_quality_bugs_tags}),
		info => _snapshot_array($product_ref->{data_quality_info_tags}),
		completeness => _snapshot_array($product_ref->{data_quality_completeness_tags}),
		warning => _snapshot_array($product_ref->{data_quality_warnings_tags}),
		error => _snapshot_array($product_ref->{data_quality_errors_tags}),
	};
}

sub _reference_result_for {
	my ($product_ref, $helper_state_ref) = @_;

	return {
		code => $product_ref->{code},
		enriched_snapshot => _enriched_snapshot_for($product_ref, $helper_state_ref),
		legacy_check_tags => _legacy_check_tags_for($product_ref),
	};
}

while (my $line = <$input_fh>) {
	chomp $line;
	next if $line !~ /\S/;

	my $product_ref = $json->decode($line);

	if (not defined $product_ref->{product_type}) {
		$product_ref->{product_type} = $options{product_type};
	}
	normalize_product_data($product_ref);
	analyze_and_enrich_product_data($product_ref, {});

	my ($minimum_number_of_ingredients, $minimum_number_category_id)
		= get_inherited_property_from_categories_tags($product_ref, "minimum_number_of_ingredients:en");
	my ($ignore_energy_calculated_error, $ignore_energy_category_id)
		= get_inherited_property_from_categories_tags($product_ref, "ignore_energy_calculated_error:en");
	my $animal_origin_categories = get_all_tags_having_property($product_ref, "categories", "food_of_animal_origin:en");
	my $helper_state = {
		is_european_product => is_european_product($product_ref) ? JSON::PP::true : JSON::PP::false,
		has_animal_origin_category => (scalar keys %{$animal_origin_categories}) > 0 ? JSON::PP::true : JSON::PP::false,
		minimum_number_of_ingredients => $minimum_number_of_ingredients,
		ignore_energy_calculated_error => ($ignore_energy_calculated_error and $ignore_energy_calculated_error eq "yes")
			? JSON::PP::true
			: JSON::PP::false,
	};

	print {$output_fh} $json->encode(
		{
			contract_kind => $result_contract_kind,
			contract_version => $result_contract_version,
			reference_result => _reference_result_for($product_ref, $helper_state),
		}
	) . "\n";
}

close $input_fh;
close $output_fh;
