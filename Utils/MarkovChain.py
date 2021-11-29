import numpy as np 
import pandas as pd 
from pychattr.channel_attribution import MarkovModel


def MarkovModel(df_paths):
	'''
	Calculate the removal effect and share in result based on the consumer journey dataframe.

	Args:
		df_paths: A dataframe contains mobile, consumer journey path and key touchpoint label (if_go_store:1/0)
		Example:
		   Mobile	                           path	                                    conversion
	     1354795436	       0110001100_tp, 0110001100_tp, O0110001100_tp, ...	             1
	     13345678920	       0123456789_tp, 0110001100_tp, 0110001100_tp, ...	             0
	'''
	## set up input dataframe
	data = {"path": df_paths.path.values.tolist(),
	"conversions": df_paths.conversion.values.tolist()}
	df = pd.DataFrame(data)

	## set up configs
	path_feature= "path"
	conversion_feature="conversions"
	null_feature = None
	max_steps = None
	separator=","
	k_order=1
	n_simulations=10000
	return_transition_probs=True
	random_state=24

	# instantiate the model
	mm = MarkovModel(path_feature=path_feature,
	                 conversion_feature=conversion_feature,
	                 null_feature=null_feature,
	                 separator=separator,
	                 k_order=k_order,
	                 n_simulations=n_simulations,
	                 max_steps=max_steps,
	                 return_transition_probs=return_transition_probs,
	                 random_state=random_state)

	# fit the model
	mm.fit(df)

	## get removal effects
	remove_effects = mm.removal_effects_.merge(id_mapping[['touchpoint_id','touchpoint_name']],left_on = 'channel_name',right_on = 'touchpoint_id').sort_vales(by = 'removal_effect',ascending = False)[['touchpoint_id','touchpoint_name','removal_effect']]

	## get share in results
	remove_effects['share_in_result'] = remove_effects['removal_effect'] / remove_effects.removal_effect.sum()

	return remove_effects